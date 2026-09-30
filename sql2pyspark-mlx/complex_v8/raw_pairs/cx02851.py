from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 4868.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

tagged = (
    scoped_claims
    .select(
        "*",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

status_stats = (
    tagged
    .groupBy("status")
    .agg(F.max("approved_amount_adj").alias("max_approved_amount_adj"))
)

vs_group = (
    tagged.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount_adj") == F.col("g.max_approved_amount_adj"))
    .select(
        "s.*",
        "g.max_approved_amount_adj",
        F.round(F.col("s.approved_amount_adj") - F.col("g.max_approved_amount_adj"), 2).alias("approved_amount_adj_minus_max"),
    )
)

result = (
    vs_group
    .cube("status", "adjuster_id", "policy_id")
    .agg(
        F.countDistinct("status_or_unknown").alias("distinct_status_or_unknowns"),
        F.round(100 * F.count(F.when(F.col("status") == "OPEN", 1)) / F.count("*"), 1).alias("pct_status_open"),
    )
)
