from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type")
w7 = Window.orderBy("event_time_hour", "id")

filtered = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/search"))) & (F.col("duration_sec") >= 438))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("duration_sec") >= 407, "High").when(F.col("duration_sec") >= 202, "Medium").otherwise("Low").alias("duration_sec_tier"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

base = (
    filtered
    .filter((F.col("event_time") > F.lit("2023-03-25").cast("date")) & (F.col("event_time") >= "2024-08-02"))
    .withColumn("event_time_hour", F.hour("event_time"))
    .withColumn("page_lower", F.lower("page"))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_prev = sessions_web_events.withColumn("prev_event_time", F.lag("event_time").over(w3))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w4))

scored = (
    islands
    .select(
        "id",
        "event_time",
        "duration_sec",
        "duration_sec_tier",
        "resolved_page",
        "event_time_hour",
        "page_lower",
        "session_id",
        "session_no",
        F.last("event_time_hour", ignorenulls=True).over(w5).alias("last_known_event_time_hour"),
        F.count("event_time_hour").over(w6).alias("event_type_count_event_time_hour"),
        F.ntile(4).over(w7).alias("event_time_hour_bucket"),
    )
)

result = (
    scored
    .groupBy("session_id")
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("duration_sec_bit_and") > 1)
    .orderBy(F.col("avg_duration_sec").desc_nulls_first())
)
