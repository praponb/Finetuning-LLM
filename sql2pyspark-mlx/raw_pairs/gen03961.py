from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w_running = Window.orderBy("pressure", "id")
w3 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("humidity", "id").rowsBetween(-6, -1)
w5 = Window.orderBy(F.col("pressure").desc_nulls_first())
w6 = Window.orderBy("prev_pressure", "id")
w_hist = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") + F.col("status_bits")).alias("temp_c_plus_status_bits"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "temp_c", "humidity", "pressure", "firmware")
)

scored = (
    latest
    .select(
        "*",
        F.lag("pressure", 1).over(w_running).alias("prev_pressure"),
        F.sum("pressure").over(w3).alias("running_pressure"),
    )
)

scored_device_readings = (
    scored
    .select(
        "id",
        "humidity",
        "pressure",
        "firmware",
        "prev_pressure",
        "running_pressure",
        F.avg("running_pressure").over(w4).alias("running_pressure_avg_6_prior"),
        F.cume_dist().over(w5).alias("pressure_cume_dist"),
        F.ntile(3).over(w6).alias("prev_pressure_bucket"),
    )
)

with_windows = (
    scored_device_readings
    .select(
        "id",
        "humidity",
        "pressure",
        "firmware",
        "running_pressure_avg_6_prior",
        "pressure_cume_dist",
        "prev_pressure_bucket",
        F.ntile(10).over(w6).alias("prev_pressure_bucket_2"),
        F.sum("running_pressure_avg_6_prior").over(w_hist).alias("running_running_pressure_avg_6_prior"),
        F.row_number().over(w_running).alias("row_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("humidity").between(40, 44))
    .select(
        "*",
        (F.col("prev_pressure_bucket") + F.col("running_running_pressure_avg_6_prior")).alias("prev_pressure_bucket_plus_running_running_pressure_avg_6_prior"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "pressure",
        "firmware",
        "running_pressure_avg_6_prior",
        "pressure_cume_dist",
        "prev_pressure_bucket",
        "prev_pressure_bucket_2",
        "running_running_pressure_avg_6_prior",
        "row_seq",
    )
)
