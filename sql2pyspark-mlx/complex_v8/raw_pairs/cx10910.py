from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")
w_event_type_n_actor_customer = Window.partitionBy("event_type", "n_actor_customer").orderBy(F.col("n_actor_customer").desc(), "actor", "event_type", "ticket_id")
w_event_type_n_actor_customer_2 = Window.partitionBy("event_type", "n_actor_customer").orderBy(F.col("n_actor_customer").desc(), "actor", "event_type", "ticket_id").rowsBetween(-6, Window.currentRow)
w_event_type_n_actor_customer_3 = Window.partitionBy("event_type", "n_actor_customer").orderBy(F.col("n_actor_customer").desc(), "actor", "event_type", "ticket_id").rowsBetween(-6, -1)
w_actor_agent_team_by_n_event = Window.partitionBy("actor", "agent_team").orderBy("n_event_type_in_resolved_assig", "actor", "event_type", "ticket_id")
w_n_event_type_in_resolved_ass = Window.partitionBy("n_event_type_in_resolved_assig", "ticket_id")

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

rollup_base = (
    filtered
    .groupBy("actor", "event_type", "ticket_id")
    .agg(
        F.sum(F.when(F.col("actor") == "customer", 1).otherwise(0)).alias("n_actor_customer"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.count(F.when(F.col("event_type").isin("resolved", "assigned"), 1)).alias("n_event_type_in_resolved_assig"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

ranked = (
    rollup_base
    .select(
        "actor",
        "event_type",
        "ticket_id",
        "n_actor_customer",
        "distinct_actors",
        "n_event_type_in_resolved_assig",
        "n_rows",
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.ntile(10).over(w_event_type_n_actor_customer).alias("n_actor_customer_decile"),
        F.round(F.avg("n_rows").over(w_event_type_n_actor_customer_2), 2).alias("n_rows_avg_7"),
        F.round(F.avg("distinct_actors").over(w_event_type_n_actor_customer_3), 2).alias("distinct_actors_avg_6_prior"),
        F.size(F.collect_set("event_type").over(w_actor)).alias("distinct_event_type_in_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
    )
)

summarized_rows = (
    ranked.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

combined_details_ticket = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "upgrade"))
    .select(
        "s.actor",
        "s.event_type",
        "s.ticket_id",
        "s.n_actor_customer",
        "s.n_event_type_in_resolved_assig",
        "s.n_rows",
        "s.distinct_ticket_id_in_actor",
        "s.n_actor_customer_decile",
        "s.distinct_actors_avg_6_prior",
        "s.distinct_event_type_in_actor",
        "s.event_actor_count",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

qualified = (
    combined_details_ticket
    .crossJoin(combined_details_ticket.agg(F.avg("event_actor_count").alias("overall_avg_event_actor_count")))
    .filter(F.col("event_actor_count") > F.col("overall_avg_event_actor_count"))
    .drop("overall_avg_event_actor_count")
)

metrics = (
    qualified
    .select(
        "actor",
        "event_type",
        "ticket_id",
        "n_actor_customer",
        "n_event_type_in_resolved_assig",
        "n_rows",
        "distinct_ticket_id_in_actor",
        "n_actor_customer_decile",
        "distinct_actors_avg_6_prior",
        "event_actor_count",
        "ticket_csat",
        "agent_tier",
        F.rank().over(w_actor_agent_team_by_n_event).alias("n_event_type_in_resolved_assig_rank"),
        F.round(F.col("n_actor_customer") / F.nullif(F.sum("n_actor_customer").over(w_n_event_type_in_resolved_ass), F.lit(0)), 4).alias("n_actor_customer_share_of_n_event_type_in_resolv"),
        F.lag("ticket_csat").over(w_actor_agent_team_by_n_event).alias("prev_ticket_csat"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_actors_avg_6_prior") < 7.8)
    .select(
        "actor",
        "event_type",
        "ticket_id",
        "n_actor_customer",
        "n_event_type_in_resolved_assig",
        "n_rows",
        "distinct_ticket_id_in_actor",
        "n_actor_customer_decile",
        "distinct_actors_avg_6_prior",
        "ticket_csat",
        "agent_tier",
        "n_actor_customer_share_of_n_event_type_in_resolv",
        "prev_ticket_csat",
        F.concat_ws("-", F.col("event_type"), F.col("agent_tier")).alias("event_type_agent_tier_label"),
    )
    .orderBy(F.col("n_actor_customer_share_of_n_event_type_in_resolv").desc())
)
