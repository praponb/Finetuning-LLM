from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("status").isin("OPEN", "PAID"), 1)).alias("claim_status_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_date",
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

per_group = (
    enriched
    .groupBy(
        F.date_trunc("month", "first_claim_date").alias("first_claim_date_month"),
    )
    .agg(
        F.sum("approved_amount").alias("total_approved_amount"),
        F.sum("claim_count").alias("total_claim_count"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.max("total_claim_count").alias("overall_max_total_claim_count")))
    .filter(F.col("total_claim_count") < F.col("overall_max_total_claim_count"))
    .drop("overall_max_total_claim_count")
)

result = (
    without_matches
    .select(
        "first_claim_date_month",
        "total_approved_amount",
        "total_claim_count",
        F.round(F.col("total_approved_amount") * 0.25, 2).alias("total_approved_amount_adj"),
    )
    .orderBy(
        "total_approved_amount_adj",
        F.col("total_approved_amount").desc_nulls_first(),
    )
)
