from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tier_group").orderBy(F.col("median_ticket_count").desc(), F.col("id").desc())

base = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("I%"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.when(F.col("tier").isin("T3", "T2"), "group_a").when(F.col("tier") == "T1", "group_b").otherwise("other").alias("tier_group"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

tier_stats = (
    base
    .groupBy("tier")
    .agg(F.max("days_since_hired_on").alias("max_days_since_hired_on"))
)

vs_group = (
    base.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.days_since_hired_on") == F.col("g.max_days_since_hired_on"))
    .select(
        "s.*",
        "g.max_days_since_hired_on",
        F.round(F.col("s.days_since_hired_on") - F.col("g.max_days_since_hired_on"), 2).alias("days_since_hired_on_minus_max"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("csat").alias("max_ticket_csat"),
    )
)

with_counts = (
    vs_group.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_csat",
    )
)

tier_stats_2 = (
    with_counts
    .groupBy("tier")
    .agg(F.percentile_approx("ticket_count", 0.5).alias("median_ticket_count"))
)

compared = (
    with_counts.alias("s")
    .join(tier_stats_2.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.ticket_count") >= F.col("g.median_ticket_count"))
    .select(
        "s.*",
        "g.median_ticket_count",
        F.round(F.col("s.ticket_count") - F.col("g.median_ticket_count"), 2).alias("ticket_count_minus_median"),
    )
)

team_stats = (
    compared
    .groupBy("team")
    .agg(F.avg("max_ticket_csat").alias("avg_max_ticket_csat"))
)

compared_2 = (
    compared.alias("s")
    .join(team_stats.alias("g"), F.col("g.team") == F.col("s.team"), "inner")
    .filter(F.col("s.max_ticket_csat") > F.col("g.avg_max_ticket_csat"))
    .select(
        "s.*",
        "g.avg_max_ticket_csat",
        F.round(F.col("s.max_ticket_csat") - F.col("g.avg_max_ticket_csat"), 2).alias("max_ticket_csat_minus_avg"),
    )
)

unique_rows = (
    compared_2
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "tier_group",
        "team_slug",
        "days_since_hired_on",
        "max_days_since_hired_on",
        "ticket_count",
        "max_ticket_csat",
        "ticket_count_minus_median",
        "avg_max_ticket_csat",
        F.coalesce(F.col("tier_group"), F.lit("UNKNOWN")).alias("tier_group_or_unknown"),
        F.when(F.col("median_ticket_count") >= 5.2, "A").when(F.col("median_ticket_count") >= 3.6, "B").otherwise("C").alias("median_ticket_count_tier"),
    )
    .orderBy(F.col("team_slug").desc(), F.col("tier_group_or_unknown").desc())
)
