from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("claimed_amount") >= 19347.2) | (F.col("approved_amount") > 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("year", F.col("reported_at")).alias("reported_at_year"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
)

enriched = (
    prepared
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.when(F.col("claim_type").isin("fire", "flood"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

with_details_adjuster = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_year",
        "s.approved_amount_bucket",
        "s.claim_date_quarter",
        "s.status_ref",
        "s.claim_type_group",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("p.status").alias("policy_status"),
    )
)

combined_details = (
    with_details_adjuster.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at_year",
        "s.approved_amount_bucket",
        "s.claim_date_quarter",
        "s.status_ref",
        "s.adjuster_team",
        "s.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team_2"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at_year",
        "approved_amount_bucket",
        "claim_date_quarter",
        "status_ref",
        "adjuster_team",
        "adjuster_level",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
    .orderBy(F.col("status_clean").desc(), F.col("id").desc())
)
