from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code")
w5 = Window.orderBy("session_id", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("session_id").desc(), "id")
w7 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy(F.col("session_start_flag").desc(), "id")
w9 = Window.partitionBy("device_code").orderBy("dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("dedup_rank", "id")
w11 = Window.partitionBy("device_code").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_device_readings
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

flagged = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("session_start_flag").over(w3))

gaps_2 = (
    sessions_device_readings
    .withColumn("session_start_flag_2", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands_device_readings = gaps_2.withColumn("island_id_2", F.sum("session_start_flag_2").over(w3))

enriched = (
    islands_device_readings
    .withColumn("device_code_max_status_bits", F.max("status_bits").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("first_known_session_start_flag", F.first("session_start_flag", ignorenulls=True).over(w7))
)

latest_device_readings = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest_device_readings
    .select(
        "*",
        F.avg("device_code_max_status_bits").over(w9).alias("device_code_max_status_bits_avg_5"),
        F.row_number().over(w10).alias("event_seq_2"),
        F.sum("island_id_2").over(w11).alias("running_island_id_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "row_num",
        "is_new_burst",
        "session_start_flag",
        "island_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-03)$"), "CORE").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("session_start_flag").desc_nulls_last(), F.col("firmware").desc())
)
