from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("temp_c_plus_humidity", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("session_start_flag").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w8 = Window.partitionBy("quarter_start").orderBy(F.col("dv_03_events").desc())

prev_events = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .filter(F.col("pressure") <= 1002)
    .withColumn("next_pressure", F.lead("pressure").over(w))
    .withColumn("prev_temp_c", F.lag("temp_c").over(w))
    .withColumn("next_temp_c", F.lead("temp_c").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

cleaned = (
    with_windows
    .filter(F.col("next_pressure") <= 1023)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "prev_reading_ts",
        "session_start_flag",
        "prev_temp_c",
        "next_temp_c",
        "seq_no",
        (F.col("temp_c") + F.col("humidity")).alias("temp_c_plus_humidity"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("status_bits").over(w_recent).alias("next_status_bits"),
        F.lag("status_bits").over(w_recent).alias("prev_status_bits"),
        F.lead("seq_no").over(w_recent).alias("next_seq_no"),
        F.row_number().over(w_recent).alias("seq_no_2"),
        F.ntile(3).over(w4).alias("session_start_flag_bucket"),
    )
)

prev_events_2 = enriched.withColumn("prev_prev_reading_ts", F.lag("prev_reading_ts").over(w5))

gaps = (
    prev_events_2
    .withColumn("is_new_burst", F.when(F.col("prev_prev_reading_ts").isNull() | (F.col("prev_reading_ts") > F.col("prev_prev_reading_ts") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w6))

latest = (
    sessionized
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "prev_prev_reading_ts").alias("quarter_start"),
        F.date_trunc("YEAR", "prev_prev_reading_ts").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("humidity")).otherwise(0)).alias("dv_04_humidity"),
        F.min("is_new_burst").alias("min_is_new_burst"),
        F.count("*").alias("event_count"),
        F.min("prev_reading_ts").alias("first_prev_reading_ts"),
        F.max("prev_reading_ts").alias("ended_at"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
    )
    .withColumn("dv_03_events_dense_rank", F.dense_rank().over(w8))
)
