from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("humidity_per_status_bits", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("event_seq").desc(), "id")
w6 = Window.orderBy("event_seq", "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("event_seq").desc(), "id")

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

valid = (
    sessions_device_readings
    .filter(F.col("island_id") == 4)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

scored = (
    valid
    .select(
        "id",
        "device_code",
        "firmware",
        "humidity_per_status_bits",
        F.row_number().over(w3).alias("event_seq"),
        F.count("pressure").over(w4).alias("pressure_count_30m"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

valid_device_readings = (
    ranked
    .filter(F.col("firmware").isin(["1.1.0", "2.0.1", "1.0.3"]))
    .select(
        "id",
        "humidity_per_status_bits",
        "event_seq",
        "row_num",
        F.least("pressure_count_30m", "event_seq").alias("least_pressure_count_30m_event_seq"),
    )
)

result = (
    valid_device_readings
    .select(
        "id",
        "least_pressure_count_30m_event_seq",
        F.lag("event_seq", 1).over(w6).alias("prev_event_seq"),
        F.sum("event_seq").over(w7).alias("cumulative_event_seq"),
        F.ntile(3).over(w8).alias("event_seq_bucket"),
    )
)
