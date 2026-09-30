from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "actor_group").orderBy("event_at", "id")

prepared_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

latest = (
    prepared_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent_ticket = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_group",
        F.col("t.closed_at").alias("ticket_closed_at"),
        "su.agent_name",
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "event_type",
        "actor_group",
        "ticket_closed_at",
    )
)
