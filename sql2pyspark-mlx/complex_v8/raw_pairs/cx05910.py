from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

derived = (
    sessionized
    .filter(F.col("session_no") < 5)
    .select(
        "*",
        F.round(F.col("starts_session") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("starts_session_per_session_no"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "resolved") & (~F.col("t.actor").isin("system", "agent")), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "prev_event_at",
        "starts_session",
        "starts_session_per_session_no",
        "actor_prefix",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("prev_event_at"))) / 60, 1).alias("minutes_prev_event_at_to_event_at"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
    .orderBy(F.col("starts_session").asc(), F.col("id").desc())
)
