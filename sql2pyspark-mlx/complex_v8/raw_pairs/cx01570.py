from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_event_at = Window.partitionBy("agent_tier").orderBy(F.col("event_at").desc(), "id")
w_agent_tier_by_event_at_ties = Window.partitionBy("agent_tier").orderBy(F.col("event_at").desc())

cleaned = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
    )
)

sequenced = (
    with_details_ticket
    .select(
        "*",
        F.lag("event_at", 3).over(w_agent_tier_by_event_at).alias("prev3_event_at"),
        F.dense_rank().over(w_agent_tier_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "reopened") & (F.hour(F.col("t.event_at")).between(6, 10)), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["actor", "event_at_dense_rank"], ["actor"], ["event_at_dense_rank"]],
        "actor", "event_at_dense_rank",
    )
    .agg(
        F.min("prev3_event_at").alias("first_prev3_event_at"),
        F.sum(F.when(F.col("actor").isin("system", "customer"), F.col("event_at_dense_rank")).otherwise(0)).alias("event_at_dense_rank_actor_in_system_customer"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "event_at_dense_rank",
        "first_prev3_event_at",
        "event_at_dense_rank_actor_in_system_customer",
    )
)
