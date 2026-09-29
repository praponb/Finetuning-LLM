from pyspark.sql import functions as F

valid = df_support_agents

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("opened_at").alias("last_ticket_opened_at"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
        "t.last_ticket_opened_at",
    )
)

totals = (
    summarized_rows
    .groupBy(
        F.date_format("last_ticket_opened_at", "yyyy-MM").alias("last_ticket_opened_at_ym"),
        "ticket_count",
    )
    .agg(
        F.round(F.avg("ticket_count"), 2).alias("avg_ticket_count"),
        F.min("last_ticket_closed_at").alias("first_last_ticket_closed_at"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.count(F.when(F.lower(F.col("agent_name")).like("%ri%"), 1)).alias("n_lower_agent_name_like_ri"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.max("avg_ticket_count").alias("overall_max_avg_ticket_count")))
    .filter(F.col("avg_ticket_count") < F.col("overall_max_avg_ticket_count"))
    .drop("overall_max_avg_ticket_count")
)

without_matches = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("ticket_count").alias("overall_avg_ticket_count")))
    .filter(F.col("ticket_count") > F.col("overall_avg_ticket_count"))
    .drop("overall_avg_ticket_count")
)

result = (
    without_matches
    .groupingSets(
        [["ticket_count", "distinct_tiers", "n_lower_agent_name_like_ri"], ["ticket_count"], []],
        "ticket_count", "distinct_tiers", "n_lower_agent_name_like_ri",
    )
    .agg(
        F.round(F.stddev("ticket_count"), 2).alias("stddev_ticket_count"),
        F.min("first_last_ticket_closed_at").alias("first_first_last_ticket_closed_at"),
    )
    .orderBy(
        F.col("ticket_count").asc_nulls_last(),
        F.col("distinct_tiers").asc_nulls_last(),
        F.col("n_lower_agent_name_like_ri").asc_nulls_last(),
    )
)
