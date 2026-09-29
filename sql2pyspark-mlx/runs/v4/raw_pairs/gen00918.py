from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("burst_id").desc(), "id")
w_part = Window.partitionBy("device_code").orderBy("corr_status_bits_humidity", "id")
w7 = Window.orderBy("humidity", "id")
w_hist = Window.partitionBy("device_code").orderBy(F.col("status_bits_bucket").desc(), "id")
w_recent = Window.orderBy(F.col("status_bits_bucket").desc(), "id")
w10 = Window.partitionBy("device_code").orderBy("prev_is_new_burst", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("device_code").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("device_code").orderBy("temp_c_scaled", "id")

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

enriched = (
    sessions_device_readings
    .select(
        "*",
        F.corr("status_bits", "humidity").over(w3).alias("corr_status_bits_humidity"),
        F.ntile(10).over(w4).alias("status_bits_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("prev_is_new_burst", F.lag("is_new_burst", 2, 0).over(w_part))
    .withColumn("humidity_bucket", F.ntile(5).over(w7))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("next_humidity", F.lead("humidity", 1).over(w_part))
    .withColumn("event_seq_2", F.row_number().over(w_recent))
)

flagged = (
    scored
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id_2", F.sum("new_group_flag").over(w2))

cleaned = (
    sessionized
    .filter(F.col("reading_ts") > F.lit("2025-09-01 10:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "is_new_burst",
        "burst_id",
        "corr_status_bits_humidity",
        "rank_in_group",
        "prev_is_new_burst",
        "humidity_bucket",
        "event_seq",
        "next_humidity",
        "event_seq_2",
        "burst_id_2",
        F.round((F.col("temp_c") * 1.15), 2).alias("temp_c_scaled"),
    )
)

base = (
    cleaned
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("burst_id") <= 3))
    .withColumn("temp_c_scaled_int", F.col("temp_c_scaled").cast("int"))
)

result = (
    base
    .select(
        "*",
        F.avg("event_seq").over(w10).alias("event_seq_avg_6_prior"),
        F.first("temp_c_scaled_int", ignorenulls=True).over(w11).alias("first_known_temp_c_scaled_int"),
        F.lead("event_seq_2", 2).over(w12).alias("next_event_seq_2"),
    )
)
