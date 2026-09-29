from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "bug")) & (F.hour(F.col("closed_at")).between(10, 14)))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
    )
)

agent_id_stats = (
    filtered_tickets
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    filtered_tickets.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

combined_details = (
    outliers.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags_text",
        "s.opened_at_date",
        "s.avg_csat",
        "s.csat_minus_avg",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

labeled = combined_details.withColumn("agent_hired_on_month_end", F.last_day(F.col("agent_hired_on")))

result = (
    labeled
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags_text",
        "opened_at_date",
        "avg_csat",
        "csat_minus_avg",
        "agent_hired_on",
        "agent_hired_on_month_end",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
        F.least(F.col("csat_minus_avg"), F.col("avg_csat")).alias("least_csat_minus_avg_avg_csat"),
    )
)
