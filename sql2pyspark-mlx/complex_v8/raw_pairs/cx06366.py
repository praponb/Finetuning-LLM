from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("status") == "CANCELLED") | (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.when(F.col("premium") >= 3465.4, "HIGH").when(F.col("premium") >= 1541.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.count("*").alias("policy_count"),
        F.sum("premium").alias("total_policy_premium"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.total_policy_premium",
    )
)

combined_details_holder = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.lower(F.col("p.holder_name")).like("%rt%")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.premium_tier",
        "s.avg_policy_premium",
        "s.policy_count",
        "s.total_policy_premium",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    combined_details_holder
    .groupingSets(
        [["status", "holder_segment"], ["status"], []],
        "status", "holder_segment",
    )
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.round(100 * F.count(F.when(F.col("holder_segment") == "SME", 1)) / F.count("*"), 1).alias("pct_holder_segment_sme"),
        F.max("start_date").alias("last_start_date"),
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
