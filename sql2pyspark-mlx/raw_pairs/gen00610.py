from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("island_id")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w7 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

latest = (
    islands
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    latest
    .filter(F.col("island_id") <= 5)
    .withColumn("first_known_is_new_burst", F.first("is_new_burst", ignorenulls=True).over(w4))
    .withColumn("temp_c_share", F.round((F.col("temp_c") / F.sum("temp_c").over(w5)), 4))
)

with_prev = with_windows.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands_device_readings = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

flagged = (
    islands_device_readings
    .withColumn("new_group_flag", F.when(F.lag("prev_reading_ts").over(w6).isNull() | (F.col("prev_reading_ts") > F.lag("prev_reading_ts").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "is_new_burst",
        "first_known_is_new_burst",
        "prev_reading_ts",
        "is_new_session",
        "session_id",
        F.sum("new_group_flag").over(w7).alias("island_id_2"),
    )
)

wide = (
    sessions_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-01"])
    .agg(F.sum("is_new_session"))
    .select("firmware", F.col("DV-02").alias("dv_02"), F.col("DV-01").alias("dv_01"))
)

result = (
    wide
    .select("firmware", "dv_01")
    .orderBy("dv_01", "firmware")
)
