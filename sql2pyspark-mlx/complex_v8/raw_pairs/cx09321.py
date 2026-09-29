from pyspark.sql import functions as F

recent = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
    )
)

policy_id_stats = (
    recent
    .groupBy("policy_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

vs_group = (
    recent.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3) & (F.col("a.team").isin("Property", "Health")), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["policy_id", "adjuster_id", "claim_type"], ["policy_id"]],
        "policy_id", "adjuster_id", "claim_type",
    )
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
    )
    .select(
        "policy_id",
        "adjuster_id",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "claim_type_list",
        "total_approved_amount",
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
