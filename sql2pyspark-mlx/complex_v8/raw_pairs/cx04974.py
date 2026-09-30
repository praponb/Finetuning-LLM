from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .filter((F.col("actor") == "customer") | (F.col("event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_actor_by_event_at).alias("seq_no"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at_2"))
)

summarized_rows = (
    sequenced.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at_2")
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_at_rank",
        "s.event_at_cume_dist",
        "s.event_at_rank_2",
        "s.first_event_at",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["event_at_rank", "ticket_id", "agent_team"], ["event_at_rank"], ["ticket_id"]],
        "event_at_rank", "ticket_id", "agent_team",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
    )
    .select(
        "event_at_rank",
        "ticket_id",
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        "grouping_level",
        "total_event_at_rank",
    )
    .orderBy(
        F.col("event_at_rank").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)
