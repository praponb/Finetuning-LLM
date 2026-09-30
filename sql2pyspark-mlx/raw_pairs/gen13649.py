from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("humidity")
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").asc_nulls_first(), "id")
w_running = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_recent = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

metrics = (
    df_device_readings
    .withColumn("temp_c_count_1h", F.count("temp_c").over(w))
    .withColumn("humidity_dense_rank", F.dense_rank().over(w_grp))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "temp_c_count_1h",
        "rn",
        (F.datediff(F.current_date(), "reading_ts") / 365.25).alias("years_since_reading_ts"),
    )
)

metrics_device_readings = (
    cleaned
    .select(
        "*",
        F.lead("temp_c").over(w_running).alias("next_temp_c"),
        F.lead("years_since_reading_ts").over(w_running).alias("next_years_since_reading_ts"),
        F.row_number().over(w_running).alias("event_seq"),
        F.avg("years_since_reading_ts").over(w_recent).alias("years_since_reading_ts_avg_2h"),
    )
)

result = (
    metrics_device_readings
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.sum("next_temp_c").alias("total_next_temp_c"),
        F.count("*").alias("cnt"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
    .filter(F.col("total_next_temp_c") < 1000000)
    .orderBy(F.col("cnt").desc())
)
