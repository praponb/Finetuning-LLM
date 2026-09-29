from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("adjuster_id") <= 5) & (F.col("status").isin("IN_REVIEW", "OPEN", "APPROVED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

enriched_adjuster = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_label",
        "s.approved_amount_filled",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

rollup_base = (
    enriched_adjuster
    .groupBy("adjuster_id")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.sum("approved_amount").alias("total_approved_amount"),
    )
)

result = (
    rollup_base
    .select(
        "adjuster_id",
        "total_approved_amount",
        F.abs(F.col("total_approved_amount") - 195000.0).alias("total_approved_amount_distance"),
        F.array_join(F.array_sort(F.col("status_list")), ",").alias("status_list_text"),
    )
)
