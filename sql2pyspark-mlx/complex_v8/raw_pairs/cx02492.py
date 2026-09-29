from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claimed_amount") <= 31752.9) & (F.hour(F.col("reported_at")).between(6, 9)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    filtered_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.min_claim_claimed_amount", "c.last_claim_reported_at")
)

qualified = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") < 20880.4), "left_semi")
)

enriched_adjuster = (
    qualified.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_dayofweek",
        "s.least_approved_amount_claimed_amount",
        "s.min_claim_claimed_amount",
        "s.last_claim_reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
    )
)

summarized_rows = (
    enriched_adjuster.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

labeled = (
    summarized_rows
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "reported_at_dayofweek",
        "least_approved_amount_claimed_amount",
        "min_claim_claimed_amount",
        "last_claim_reported_at",
        "adjuster_name",
        "adjuster_level",
        "claim_status_count",
        (F.col("approved_amount") + F.col("adjuster_level")).alias("approved_amount_plus_adjuster_level"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

derived = (
    labeled
    .filter(F.col("approved_amount_plus_adjuster_level") >= 17501.8)
    .withColumn("min_claim_claimed_amount_adj", F.round(F.col("min_claim_claimed_amount") * 1.5, 2))
)

result = (
    derived
    .filter(F.col("claim_status_count") < 6)
    .select(
        "id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "reported_at_dayofweek",
        "least_approved_amount_claimed_amount",
        "min_claim_claimed_amount",
        "last_claim_reported_at",
        "adjuster_name",
        "adjuster_level",
        "claim_status_count",
        "status_clean",
        "min_claim_claimed_amount_adj",
        F.round((F.unix_timestamp(F.col("reported_at")) - F.unix_timestamp(F.col("last_claim_reported_at"))) / 60, 1).alias("minutes_last_claim_reported_at_to_reported_at"),
    )
    .orderBy("claim_status_count", F.col("min_claim_claimed_amount_adj").desc())
)
