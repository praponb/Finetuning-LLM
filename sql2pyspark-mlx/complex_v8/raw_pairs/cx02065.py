from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount").between(10673.3, 34401.0)) & (F.hour(F.col("reported_at")).between(9, 14)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.abs(F.col("claimed_amount") - 32441.2).alias("claimed_amount_distance"),
    )
)

combined_details = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.claimed_amount_filled",
        "s.claimed_amount_distance",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level_2"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    combined_details_2
    .select("claimed_amount_filled", "adjuster_team", "adjuster_name")
    .distinct()
    .orderBy(F.col("adjuster_team").asc_nulls_last())
)
