from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.when(F.col("product").isin("TRAVEL", "HOME"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

derived = (
    cleaned_policys
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

line_level = (
    derived.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.days_since_start_date",
        "s.product_or_unknown",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    line_level
    .filter(F.col("status") != "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "product_or_unknown",
        "status_slug",
        "claim_id",
        "claim_status",
        "claim_reported_at",
        "claim_approved_amount",
        "claim_claimed_amount",
    )
)
