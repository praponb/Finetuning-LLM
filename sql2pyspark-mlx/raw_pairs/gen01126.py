from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time_period", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-19 20:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("event_time_day", F.dayofmonth("event_time"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter((F.col("event_time") > F.to_date(F.lit("2023-10-16"))) & F.col("page").isNotNull())
    .withColumn("event_time_period", F.date_trunc("year", "event_time"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

gaps_2 = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("event_time_period").over(w3).isNull() | (F.col("event_time_period") > F.lag("event_time_period").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized_web_events = (
    gaps_2
    .select(
        "id",
        "event_type",
        "event_time",
        "page_clean",
        "event_time_period",
        "resolved_page",
        "session_start_flag",
        "burst_id",
        F.sum("is_new_burst").over(w4).alias("session_no"),
    )
)

flagged = (
    sessionized_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time_period") - F.lag("event_time_period").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id", F.sum("is_new_burst").over(w4))

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "page_clean",
        "event_time_period",
        "resolved_page",
        "session_start_flag",
        "burst_id",
        "is_new_burst",
        "session_id",
    )
    .orderBy(F.col("session_start_flag").desc_nulls_first(), F.col("id").desc())
    .limit(20)
)
