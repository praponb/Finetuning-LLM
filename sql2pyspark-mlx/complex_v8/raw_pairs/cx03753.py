from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("status") == "ACTIVE") & (F.col("premium") < 4417.0))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

with_children_claims = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.start_date_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

derived = (
    with_children_claims
    .filter(F.col("status") != "CANCELLED")
    .withColumn("claim_claimed_amount_distance", F.abs(F.col("claim_claimed_amount") - 20436.5))
)

enriched = (
    derived
    .select(
        "id",
        "status",
        "start_date_label",
        "claim_id",
        "claim_claimed_amount",
        "claim_reported_at",
        "claim_claimed_amount_distance",
        F.lower(F.trim(F.col("start_date_label"))).alias("start_date_label_clean"),
    )
)

with_children_claims_2 = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") <= 41878.5), "inner")
    .select(
        "s.id",
        "s.status",
        "s.start_date_label",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_claimed_amount_distance",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_type",
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_stats = (
    with_children_claims_2.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.avg_claim_approved_amount", "c.first_claim_reported_at")
)

result = (
    with_stats
    .select(
        "id",
        "status",
        "claim_id",
        "claim_claimed_amount_distance",
        "claim_id_2",
        "claim_type",
        "claim_claimed_amount_2",
        "claim_approved_amount",
        "first_claim_reported_at",
    )
    .orderBy(F.col("claim_type").desc_nulls_first())
)
