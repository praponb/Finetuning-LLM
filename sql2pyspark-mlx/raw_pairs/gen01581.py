from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code")
w_hist_2 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("device_code").orderBy("device_code_max_humidity", "id")
w4 = Window.orderBy("pressure_share", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("humidity_plus_next_pressure").desc(), "id")
w_grp = Window.partitionBy("device_code").orderBy("pressure_share", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_device_readings
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .withColumn("device_code_max_humidity", F.max("humidity").over(w_hist))
    .withColumn("status_bits_avg_5_prior", F.avg("status_bits").over(w_hist_2))
)

cleaned = (
    with_windows
    .filter(F.col("device_code") == "DV-04")
    .withColumn("firmware_length", F.length("firmware"))
    .withColumn("least_device_code_max_humidity_status_bits_avg_5_prior", F.least("device_code_max_humidity", "status_bits_avg_5_prior"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.lag("least_device_code_max_humidity_status_bits_avg_5_prior").over(w3).alias("prev_least_device_code_max_humidity_status_bits_avg_5_prior"),
        F.lead("least_device_code_max_humidity_status_bits_avg_5_prior").over(w3).alias("next_least_device_code_max_humidity_status_bits_avg_5_prior"),
        F.lead("pressure").over(w3).alias("next_pressure"),
        F.lag("pressure").over(w3).alias("prev_pressure"),
        F.row_number().over(w3).alias("row_seq"),
        F.round((F.col("pressure") / F.sum("pressure").over(w_hist)), 4).alias("pressure_share"),
    )
)

base = (
    enriched
    .filter(F.col("device_code_max_humidity") == 46)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "device_code_max_humidity",
        "status_bits_avg_5_prior",
        "firmware_length",
        "least_device_code_max_humidity_status_bits_avg_5_prior",
        "next_least_device_code_max_humidity_status_bits_avg_5_prior",
        "next_pressure",
        "prev_pressure",
        "row_seq",
        "pressure_share",
        (F.col("humidity") + F.col("next_pressure")).alias("humidity_plus_next_pressure"),
    )
)

metrics = (
    base
    .withColumn("device_code_max_humidity_share", F.round((F.col("device_code_max_humidity") / F.sum("device_code_max_humidity").over(w_hist)), 4))
    .withColumn("corr_temp_c_status_bits", F.corr("temp_c", "status_bits").over(w_hist))
    .withColumn("prev_prev_pressure", F.lag("prev_pressure", 1).over(w4))
    .withColumn("humidity_plus_next_pressure_bucket", F.ntile(10).over(w5))
)

scored = (
    metrics
    .select(
        "*",
        F.count("prev_prev_pressure").over(w_grp).alias("last7_count_prev_prev_pressure"),
        F.max("prev_prev_pressure").over(w_grp).alias("last7_max_prev_prev_pressure"),
        F.min("prev_prev_pressure").over(w_grp).alias("last7_min_prev_prev_pressure"),
        F.stddev_samp("next_least_device_code_max_humidity_status_bits_avg_5_prior").over(w_hist).alias("device_code_stddev_samp_next_least_device_code_max_humidity_status_bits_avg_5_prior"),
    )
)

daily = (
    scored
    .groupBy("device_code")
    .agg(
        F.percentile_approx("next_pressure", 0.9).alias("next_pressure_p90"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("humidity_per_dv_01"),
        F.percentile_approx("humidity_plus_next_pressure_bucket", [0.25, 0.5, 0.75]).alias("humidity_plus_next_pressure_bucket_quartiles"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    daily
    .select(
        "device_code",
        "next_pressure_p90",
        "humidity_per_dv_01",
        "num_rows",
        F.when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "OTHER_ZONE").when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "EXTENDED").otherwise("GROUP_B").alias("device_code_zone"),
    )
)
