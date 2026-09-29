from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-02-12 07:00:00").cast("timestamp")) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.when(F.col("claim_type").isin("fire", "medical"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.claim_date_quarter",
        "s.claim_type_group",
        "s.claim_date_label",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    combined_details
    .groupingSets(
        [["status", "policy_id"], ["status"], ["policy_id"], []],
        "status", "policy_id",
    )
    .agg(
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)
