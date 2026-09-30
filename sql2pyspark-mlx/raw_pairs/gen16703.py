from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("event_time_day"), "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time_day", "id")
w4 = Window.orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc(), "id")
w6 = Window.partitionBy("session_id").orderBy("event_time", "id")
w7 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

filtered_web_events = (
    filtered
    .filter(F.col("event_time") > F.lit("2023-06-07 17:00:00").cast("timestamp"))
    .withColumn("event_type_length", F.length("event_type"))
)

enriched = (
    filtered_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_type_length",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.sum("event_type_length").over(w2).alias("cumulative_event_type_length"),
        F.lag("event_type_length", 2).over(w3).alias("prev_event_type_length"),
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(4).over(w5).alias("duration_sec_bucket"),
    )
)

prev_events = enriched.withColumn("prev_event_time", F.lag("event_time").over(w6))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "event_type_length",
        "event_seq",
        F.sum("new_group_flag").over(w7).alias("session_id_2"),
    )
)

flagged = (
    sessions_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w7))

result = (
    sessions_web_events_2
    .groupBy(
        F.when(F.col("event_type_length") >= 74, "HIGH").otherwise("LOW").alias("event_type_length_tier"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.min("event_seq").alias("min_event_seq"),
    )
    .orderBy(F.col("duration_sec_bit_or").asc_nulls_first())
)
