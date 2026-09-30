from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0", "1.0.3"]))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

prev_events = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

with_prev = sessionized.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps_2 = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = gaps_2.withColumn("burst_id", F.sum("is_new_session").over(w2))

valid = (
    sessionized_device_readings
    .filter(F.col("burst_id") > 3)
    .select("*", F.substring("firmware", 1, 3).alias("firmware_prefix"))
)

base_device_readings = (
    valid
    .filter(F.col("device_code") == "DV-01")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "prev_reading_ts",
        "is_new_session",
        "burst_id",
        F.col("prev_reading_ts").cast("date").alias("prev_reading_ts_day"),
    )
)

result = (
    base_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "firmware",
        "prev_reading_ts",
        "burst_id",
        "prev_reading_ts_day",
    )
    .orderBy(F.col("firmware").asc_nulls_first(), "prev_reading_ts")
)
