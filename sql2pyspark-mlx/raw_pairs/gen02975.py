from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("temp_c", "id")
w2 = Window.orderBy(F.col("temp_c").asc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy(F.col("temp_c_dense_rank").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("pressure", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "reading_ts_day",
        F.ntile(3).over(w_part).alias("temp_c_bucket"),
        F.dense_rank().over(w2).alias("temp_c_dense_rank"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "device_code", "pressure", "temp_c_bucket")
)

enriched = (
    latest
    .select(
        "id",
        "pressure",
        F.lead("temp_c_bucket").over(w_running).alias("next_temp_c_bucket"),
        F.lag("temp_c_bucket").over(w_running).alias("prev_temp_c_bucket"),
        F.lag("pressure").over(w_running).alias("prev_pressure"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

result = (
    enriched
    .select("id", "pressure", "next_temp_c_bucket", "prev_pressure")
    .orderBy(F.col("prev_pressure").desc())
)
