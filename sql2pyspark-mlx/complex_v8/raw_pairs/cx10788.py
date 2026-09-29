from pyspark.sql import functions as F

scoped = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        F.when(F.col("status").isin("PAID", "OPEN"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.first_claim_date")
)

with_details = (
    with_counts.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.reported_at",
        "s.approved_amount_adj",
        "s.status_group",
        "s.last_claim_reported_at",
        "s.first_claim_date",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    with_details
    .groupingSets(
        [["policy_id", "status_group", "status"], ["policy_id"], ["status_group"], []],
        "policy_id", "status_group", "status",
    )
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .select(
        "policy_id",
        F.coalesce(F.col("status_group"), F.lit("ALL")).alias("status_group"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "distinct_ids",
    )
)
