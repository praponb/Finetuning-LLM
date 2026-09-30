from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("start_date").isNotNull()) & (F.col("product") == "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 1541.8, "medium").otherwise("small").alias("premium_tier"),
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier_2"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (~F.col("p.status").isin("ACTIVE", "LAPSED")), "left_anti")
    .filter(F.col("premium") > 1051.9)
)

derived = (
    with_activity
    .select(
        "id",
        "product",
        "end_date",
        "status",
        "premium_tier",
        "status_clean",
        F.abs(F.col("premium") - 2033.8).alias("premium_distance"),
    )
)

labeled = (
    derived
    .filter(F.col("end_date") < F.lit("2026-12-05").cast("date"))
    .select(
        "id",
        "premium_tier",
        "status_clean",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

with_children_claims = (
    labeled.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.status_clean",
        "s.product_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    with_children_claims
    .select(
        "id",
        "status_clean",
        "claim_id",
        "claim_status",
        "claim_claimed_amount",
        "claim_adjuster_id",
        "claim_reported_at",
    )
    .orderBy(F.col("claim_adjuster_id").desc())
)
