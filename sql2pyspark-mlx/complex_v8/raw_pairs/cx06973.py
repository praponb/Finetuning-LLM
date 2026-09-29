from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_actor_running))

joined_ticket = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "prev_event_at",
        "burst_id",
        "ticket_closed_at",
        "ticket_agent_id",
        F.when(F.col("burst_id") >= 5, "large").when(F.col("burst_id") >= 3, "medium").otherwise("small").alias("burst_id_tier"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)
