from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id")
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").desc_nulls_first())

filtered_agents = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("K%")) & (F.col("hired_on") < F.lit("2024-12-31").cast("date")))
)

metrics = (
    filtered_agents
    .select(
        "*",
        F.rank().over(w_all_by_hired_on).alias("hired_on_rank"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.avg("hired_on_rank").alias("overall_avg_hired_on_rank")))
    .filter(F.col("hired_on_rank") > F.col("overall_avg_hired_on_rank"))
    .drop("overall_avg_hired_on_rank")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    filtered.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

without_matches = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.array_contains(F.col("t.tags"), "bug")), "left_semi")
)

enriched = (
    without_matches
    .select(
        "*",
        F.when(F.col("tier").isin("T3", "T1"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
        F.when(F.col("hired_on_rank") >= 6, "HIGH").when(F.col("hired_on_rank") >= 3, "MEDIUM").otherwise("LOW").alias("hired_on_rank_tier"),
        F.round(F.col("hired_on_percent_rank") / F.nullif(F.col("ticket_count"), F.lit(0)), 3).alias("hired_on_percent_rank_per_ticket_count"),
    )
)

result = (
    enriched
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "hired_on_rank",
        "hired_on_percent_rank",
        "ticket_count",
        "tier_group",
        "hired_on_percent_rank_per_ticket_count",
    )
    .orderBy(F.col("hired_on_percent_rank").asc())
)
