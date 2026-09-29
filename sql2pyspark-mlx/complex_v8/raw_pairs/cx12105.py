from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_agent_id_ticket_id_by = Window.partitionBy("ticket_agent_id", "ticket_id").orderBy("event_at")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_agent_id_ticket_id_by_2 = Window.partitionBy("ticket_agent_id", "ticket_id").orderBy("event_at", "id")

filtered = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_priority",
        "ticket_agent_id",
        "agent_team",
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at).alias("second_event_at"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.round(F.cume_dist().over(w_ticket_agent_id_ticket_id_by), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.ntile(4).over(w_actor_event_type_by_event_at_2).alias("event_at_quartile_2"),
        F.lead("event_at").over(w_actor_event_type_by_event_at_2).alias("next_event_at"),
        F.lag("event_at").over(w_ticket_agent_id_ticket_id_by_2).alias("prev_event_at"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at_2"),
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_agent_id",
        "event_at_quartile",
        "event_at_dense_rank",
        "event_at_rank",
        "event_at_quartile_2",
        "next_event_at",
        "prev_event_at",
        "prev_event_at_2",
        "prev2_event_at",
        F.date_trunc("week", F.col("second_event_at")).alias("second_event_at_week"),
    )
    .orderBy("ticket_agent_id", "id")
    .limit(5)
)
