from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_priority_p1_row_count = Window.partitionBy("n_priority_p1", "row_count")
w_distinct_ids_n_agent_tier_in = Window.partitionBy("distinct_ids", "n_agent_tier_in_t3_t2").orderBy("n_agent_tier_in_t3_t2_share_of_n_priority_p1_row", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids = Window.partitionBy("distinct_ids")

cleaned = (
    df_tickets
    .filter((F.col("csat").between(2, 5)) & (F.month(F.col("closed_at")).isin(1, 3)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.priority_or_unknown",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

rollup_base = (
    screened
    .filter(F.col("closed_at") >= F.lit("2025-01-24 01:00:00").cast("timestamp"))
    .groupBy(F.date_trunc("month", "opened_at").alias("opened_at_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("priority") != "P1", 1)).alias("n_priority_p1"),
        F.count(F.when(F.col("agent_tier").isin("T3", "T2"), 1)).alias("n_agent_tier_in_t3_t2"),
        F.count("*").alias("row_count"),
    )
)

with_windows = (
    rollup_base
    .select(
        "opened_at_month",
        "distinct_ids",
        "n_priority_p1",
        "n_agent_tier_in_t3_t2",
        "row_count",
        F.round(F.col("n_priority_p1") - F.avg("n_priority_p1").over(w_n_priority_p1_row_count), 2).alias("n_priority_p1_vs_n_priority_p1_row_count_avg"),
        F.round(F.col("n_agent_tier_in_t3_t2") / F.nullif(F.sum("n_agent_tier_in_t3_t2").over(w_n_priority_p1_row_count), F.lit(0)), 4).alias("n_agent_tier_in_t3_t2_share_of_n_priority_p1_row"),
    )
)

without_matches = (
    with_windows
    .crossJoin(with_windows.agg(F.min("n_agent_tier_in_t3_t2_share_of_n_priority_p1_row").alias("overall_min_n_agent_tier_in_t3_t2_share_of_n_pri")))
    .filter(F.col("n_agent_tier_in_t3_t2_share_of_n_priority_p1_row") > F.col("overall_min_n_agent_tier_in_t3_t2_share_of_n_pri"))
    .drop("overall_min_n_agent_tier_in_t3_t2_share_of_n_pri")
)

with_windows_2 = (
    without_matches
    .withColumn("min_n_agent_tier_in_t3_t2_so_far", F.min("n_agent_tier_in_t3_t2").over(w_distinct_ids_n_agent_tier_in))
    .withColumn("distinct_ids_avg_n_agent_tier_in_t3_t2", F.round(F.avg("n_agent_tier_in_t3_t2").over(w_distinct_ids), 2))
)

result = (
    with_windows_2
    .select(
        "opened_at_month",
        "distinct_ids",
        "n_priority_p1",
        "n_agent_tier_in_t3_t2",
        "n_priority_p1_vs_n_priority_p1_row_count_avg",
        "n_agent_tier_in_t3_t2_share_of_n_priority_p1_row",
        "min_n_agent_tier_in_t3_t2_so_far",
        "distinct_ids_avg_n_agent_tier_in_t3_t2",
        F.abs(F.col("n_agent_tier_in_t3_t2") - 3).alias("n_agent_tier_in_t3_t2_distance"),
    )
    .orderBy(
        "n_agent_tier_in_t3_t2_share_of_n_priority_p1_row",
        F.col("distinct_ids_avg_n_agent_tier_in_t3_t2").desc_nulls_first(),
    )
)
