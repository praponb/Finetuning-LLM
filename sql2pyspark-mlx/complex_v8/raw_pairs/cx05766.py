from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_event_type_by_pre = Window.partitionBy("agent_team", "event_type").orderBy("prev3_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id_ticket_id = Window.partitionBy("ticket_agent_id", "ticket_id")

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") != "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

combined_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.lag("event_at", 3).over(w_all_by_event_at).alias("prev3_event_at"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

combined_details_2 = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.ticket_agent_id",
        "s.event_at_cume_dist",
        "s.prev_event_at",
        "s.last_event_at",
        "s.prev3_event_at",
        "s.event_at_dense_rank",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

without_matches = (
    combined_details_2.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.col("t.priority").isNotNull()), "left_semi")
    .filter(~F.col("agent_team").isin("Onboarding", "VIP"))
)

with_trends = (
    without_matches
    .select(
        "*",
        F.sum("event_at_dense_rank").over(w_agent_team_event_type_by_pre).alias("running_event_at_dense_rank"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_ticket_agent_id_ticket_id), F.lit(0)), 4).alias("event_at_cume_dist_share_of_ticket_agent_id_tick"),
    )
)

result = (
    with_trends
    .cube("event_type", "actor_or_unknown")
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.max("event_at_dense_rank").alias("max_event_at_dense_rank"),
        F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("G%"), 1)).alias("n_upper_trim_agent_name_like_g"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        F.coalesce(F.col("actor_or_unknown"), F.lit("ALL")).alias("actor_or_unknown"),
        "is_total_event_type",
        "max_event_at_dense_rank",
        "n_upper_trim_agent_name_like_g",
        "n_rows",
    )
)
