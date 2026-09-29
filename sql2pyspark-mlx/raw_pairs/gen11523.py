from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("event_time")).orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("page_upper", F.upper("page"))
    .withColumn("page_filled", F.coalesce("page", F.lit("NONE")))
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

scored = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.first("page", ignorenulls=True).over(w2).alias("first_known_page"),
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page_2"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.device"))
)

result = (
    joined
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "first_known_page",
        "first_known_page_2",
        "device",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("device").asc_nulls_last())
)
