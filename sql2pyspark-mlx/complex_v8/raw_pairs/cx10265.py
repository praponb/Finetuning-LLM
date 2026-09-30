from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 44921.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

joined_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.least_claimed_amount_approved_amount",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.product").alias("policy_product"),
    )
)

filtered = (
    joined_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") <= 23487.5), "left_anti")
)

detail_rows = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") <= 12), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.least_claimed_amount_approved_amount",
        "s.policy_start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.min("claim_date").alias("first_claim_date_2"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.claim_policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.first_claim_date_2",
        "c.last_claim_date",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "claim_id",
        "claim_policy_id",
        "claim_reported_at",
        "claim_status",
        "first_claim_date",
        "first_claim_date_2",
        F.date_trunc("month", F.col("first_claim_date_2")).alias("first_claim_date_2_month"),
    )
    .orderBy("reported_at", "id", "claim_id")
    .limit(5)
)
