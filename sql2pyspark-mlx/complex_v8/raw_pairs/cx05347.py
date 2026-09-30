from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("agent_team").orderBy(F.col("event_actor_count").desc(), F.col("ticket_id").desc(), F.col("next_event_at_year").desc())

base = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
    )
)

totals = (
    sequenced
    .filter(F.col("event_type").isin("assigned", "replied"))
    .groupBy("ticket_id", F.year("next_event_at").alias("next_event_at_year"))
    .agg(
        F.sum(F.when(~F.col("actor").isin("agent", "system"), 1).otherwise(0)).alias("n_actor_not_in_agent_system"),
        F.sum(F.when(F.col("actor") != "system", F.col("seq_no"))).alias("seq_no_actor_system"),
    )
)

without_matches = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("t.actor").isin("system", "customer")), "left_anti")
    .filter(F.col("seq_no_actor_system") <= 38)
)

enriched = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") >= 7)
    .select(
        "s.ticket_id",
        "s.next_event_at_year",
        "s.seq_no_actor_system",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        "t.first_event_at",
    )
)

unique_rows = (
    with_stats
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .filter(F.col("agent_team") == "Tech")
    .select(
        "ticket_id",
        "next_event_at_year",
        "seq_no_actor_system",
        "ticket_requester_id",
        "ticket_csat",
        "agent_name",
        "agent_team",
        "event_actor_count",
        "first_event_at",
    )
)
