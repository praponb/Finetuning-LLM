from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("approved_amount").between(2272.7, 32510.7)) & (F.col("adjuster_id") <= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

tagged = (
    base_claims
    .select(
        "*",
        F.date_add(F.col("claim_date"), 7).alias("claim_date_plus_7d"),
        F.substring(F.trim(F.col("claim_date_label")), 1, 4).alias("claim_date_label_prefix"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claim_type_prefix",
        "s.claim_date_label_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    detail_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.max_claim_claimed_amount", "c.last_claim_reported_at")
)

result = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claim_type_prefix",
        "claim_date_label_prefix",
        "claim_id",
        "claim_reported_at",
        "claim_claim_date",
        "claim_claim_type",
        "claim_status",
        "max_claim_claimed_amount",
        "last_claim_reported_at",
    )
    .orderBy(F.col("claim_claim_date").desc())
)
