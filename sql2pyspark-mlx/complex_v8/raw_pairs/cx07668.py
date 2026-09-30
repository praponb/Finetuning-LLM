from pyspark.sql import functions as F

base = (
    df_claims
    .filter(F.col("status") == "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

status_stats = (
    base
    .groupBy("status")
    .agg(F.max("approved_amount").alias("max_approved_amount"))
)

outliers = (
    base.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount") == F.col("g.max_approved_amount"))
    .select(
        "s.*",
        "g.max_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.max_approved_amount"), 2).alias("approved_amount_minus_max"),
    )
)

adjuster_id_stats = (
    outliers
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("max_approved_amount", 0.5).alias("median_max_approved_amount"))
)

compared = (
    outliers.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.max_approved_amount") >= F.col("g.median_max_approved_amount"))
    .select(
        "s.*",
        "g.median_max_approved_amount",
        F.round(F.col("s.max_approved_amount") - F.col("g.median_max_approved_amount"), 2).alias("max_approved_amount_minus_median"),
    )
)

stats = (
    compared
    .groupBy(
        "status",
        "policy_id",
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
    )
    .agg(
        F.sum("approved_amount_minus_max").alias("total_approved_amount_minus_max"),
        F.max("claim_date").alias("last_claim_date"),
        F.round(F.avg("approved_amount_filled"), 2).alias("avg_approved_amount_filled"),
        F.round(F.sum("max_approved_amount_minus_median") / F.nullif(F.sum("approved_amount_minus_max"), F.lit(0)), 3).alias("max_approved_amount_minus_median_to_approved_amo"),
        F.max("max_approved_amount_minus_median").alias("max_max_approved_amount_minus_median"),
    )
)

with_details = (
    stats.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.status",
        "s.policy_id",
        "s.claim_date_month",
        "s.total_approved_amount_minus_max",
        "s.last_claim_date",
        "s.avg_approved_amount_filled",
        "s.max_approved_amount_minus_median_to_approved_amo",
        "s.max_max_approved_amount_minus_median",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        "po.holder_name",
    )
)

combined_details_policy = (
    with_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.status",
        "s.policy_id",
        "s.claim_date_month",
        "s.last_claim_date",
        "s.max_approved_amount_minus_median_to_approved_amo",
        "s.max_max_approved_amount_minus_median",
        "s.policy_premium",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.holder_name").alias("holder_holder_name"),
        F.col("po.segment").alias("holder_segment"),
    )
)

result = (
    combined_details_policy
    .select(
        "status",
        "policy_id",
        "claim_date_month",
        "last_claim_date",
        "max_approved_amount_minus_median_to_approved_amo",
        "policy_premium",
        "policy_holder_id",
        "policy_end_date",
        "holder_holder_name",
        "holder_segment",
        F.round(F.col("policy_premium") * 1.5, 2).alias("policy_premium_adj"),
    )
)
