from pyspark.sql import functions as F

base = (
    df_policies
    .filter(F.col("premium") < 3544.1)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

combined_details_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2025-07-03").cast("date")), "inner")
    .filter(F.col("p.holder_name").isNotNull())
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

filtered = (
    combined_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") < F.lit("2025-01-10").cast("date")), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        "c.first_claim_reported_at",
        "c.first_claim_date",
    )
)

scored = (
    with_counts
    .filter(F.col("min_claim_claimed_amount") <= 45010.0)
    .select(
        "*",
        F.substring(F.trim(F.col("product_slug")), 1, 4).alias("product_slug_prefix"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

labeled = (
    scored
    .withColumn("holder_region_or_unknown", F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")))
)

result = (
    labeled
    .filter(F.col("first_claim_date") < F.lit("2025-02-01").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status_len",
        "days_start_date_to_end_date",
        "product_slug",
        "holder_name",
        "holder_risk_score",
        "min_claim_claimed_amount",
        "first_claim_reported_at",
        "end_date_week",
        "holder_region_or_unknown",
    )
    .orderBy("holder_region_or_unknown", F.col("start_date").desc_nulls_first())
)
