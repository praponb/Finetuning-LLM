from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w6 = Window.partitionBy("device_code").orderBy("last_known_pressure", "id")
w7 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("temp_c_count_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("seq_no").desc(), "id")
w_part = Window.partitionBy("device_code").orderBy("adjusted_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("last_known_pressure_per_temp_c_count_1h", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_device_readings
    .select(
        "*",
        F.corr("temp_c", "humidity").over(w).alias("corr_temp_c_humidity"),
        F.last("pressure", ignorenulls=True).over(w2).alias("last_known_pressure"),
        F.count("temp_c").over(w3).alias("temp_c_count_1h"),
    )
)

metrics = (
    enriched
    .withColumn("last_known_pressure_avg_3_prior", F.avg("last_known_pressure").over(w4))
    .withColumn("temp_c_bucket", F.ntile(3).over(w5))
)

valid = (
    metrics
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "corr_temp_c_humidity",
        "last_known_pressure",
        "temp_c_count_1h",
        "temp_c_bucket",
        F.round((F.col("last_known_pressure") / F.col("temp_c_count_1h")), 2).alias("last_known_pressure_per_temp_c_count_1h"),
    )
)

base = (
    valid
    .filter(F.col("last_known_pressure") >= 994)
    .select("*", F.round((F.col("pressure") * 100), 2).alias("adjusted_pressure"))
)

scored = (
    base
    .select(
        "*",
        F.stddev_samp("pressure").over(w).alias("device_code_stddev_samp_pressure"),
        F.row_number().over(w6).alias("seq_no"),
        F.last("pressure", ignorenulls=True).over(w7).alias("last_known_pressure_2"),
        F.sum("temp_c_bucket").over(w8).alias("cumulative_temp_c_bucket"),
    )
)

metrics_device_readings = (
    scored
    .select(
        "*",
        F.sum("temp_c").over(w).alias("device_code_sum_temp_c"),
        F.ntile(4).over(w9).alias("seq_no_bucket"),
        F.last("device_code_stddev_samp_pressure", ignorenulls=True).over(w_part).alias("last_known_device_code_stddev_samp_pressure"),
        F.avg("corr_temp_c_humidity").over(w11).alias("corr_temp_c_humidity_avg_3"),
        F.count("last_known_pressure_2").over(w).alias("device_code_count_last_known_pressure_2"),
    )
)

filtered = (
    metrics_device_readings
    .filter(F.col("pressure") >= 1026)
    .select(
        "id",
        "device_code",
        "pressure",
        "temp_c_count_1h",
        "temp_c_bucket",
        "last_known_pressure_per_temp_c_count_1h",
        "device_code_stddev_samp_pressure",
        "seq_no",
        "last_known_pressure_2",
        "cumulative_temp_c_bucket",
        "seq_no_bucket",
        "last_known_device_code_stddev_samp_pressure",
        "corr_temp_c_humidity_avg_3",
        "device_code_count_last_known_pressure_2",
        F.coalesce("cumulative_temp_c_bucket", F.lit(0)).alias("cumulative_temp_c_bucket_filled"),
    )
)

result = (
    filtered
    .groupBy("seq_no")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.percentile_approx("seq_no_bucket", [0.25, 0.5, 0.75]).alias("seq_no_bucket_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .orderBy(F.col("seq_no").asc_nulls_first())
)
