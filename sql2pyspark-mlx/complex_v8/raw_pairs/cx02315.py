from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

line_level = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("created", "resolved")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.prev_event_at",
        "s.is_new_session",
        "s.session_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

stats = (
    line_level
    .filter(F.col("event_event_type") != "replied")
    .groupBy(
        F.to_date("event_event_at").alias("event_event_at_day"),
        F.date_format("prev_event_at", "yyyy-MM").alias("prev_event_at_ym"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("event_event_type").isin("reopened", "replied"), 1)).alias("n_event_event_type_in_reopened"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("event_event_at").alias("first_event_event_at"),
        F.avg("is_new_session").alias("avg_is_new_session_check"),
    )
    .filter(F.col("avg_is_new_session_check") > 0)
    .drop("avg_is_new_session_check")
)

result = (
    stats
    .filter(F.to_date(F.col("first_event_event_at")) < F.lit("2025-01-31").cast("date"))
    .select(
        "event_event_at_day",
        "n_rows",
        "n_event_event_type_in_reopened",
        "first_event_event_at",
        F.lower(F.regexp_replace(F.trim(F.col("prev_event_at_ym")), " ", "_")).alias("prev_event_at_ym_slug"),
    )
    .orderBy(
        F.col("event_event_at_day").asc(),
        F.col("prev_event_at_ym_slug").desc(),
    )
)
