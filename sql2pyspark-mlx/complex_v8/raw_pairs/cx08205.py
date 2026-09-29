from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy("hired_on", "id")

valid_agents = (
    df_support_agents
    .filter((F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-11-09").cast("date"))) & (F.col("team").isin("VIP", "Tech", "Billing")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("tier")), 1, 3).alias("tier_prefix"),
        F.lower(F.trim(F.col("team"))).alias("team_clean"),
    )
)

deduped = (
    valid_agents
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    deduped.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    summarized_rows
    .rollup("ticket_count", "team", "tier")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.percentile_approx("ticket_count", 0.5).alias("median_ticket_count"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
    )
    .select(
        "ticket_count",
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        F.coalesce(F.col("tier"), F.lit("ALL")).alias("tier"),
        "grouping_level",
        "cnt",
        "median_ticket_count",
        "team_list",
    )
    .orderBy(
        F.col("ticket_count").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
        F.col("tier").asc_nulls_last(),
    )
)
