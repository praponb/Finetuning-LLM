from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(F.col("holder_id") <= 8)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

line_level = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.premium_distance",
        "s.days_since_end_date",
        "s.premium_adj",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
    )
)

with_details = (
    line_level.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isNotNull()), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

exploded_detail_claims = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.premium_adj",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_type",
        "s.holder_segment",
        "s.holder_joined_on",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

qualified = (
    exploded_detail_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.year(F.col("p.end_date")) == 2026), "left_semi")
)

derived = (
    qualified
    .withColumn("claim_approved_amount_per_premium_adj", F.round(F.col("claim_approved_amount") / F.nullif(F.col("premium_adj"), F.lit(0)), 3))
)

result = (
    derived
    .filter(F.col("claim_adjuster_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "end_date",
        "claim_id",
        "claim_adjuster_id",
        "claim_type",
        "holder_segment",
        "holder_joined_on",
        "claim_id_2",
        "claim_date",
        "claim_adjuster_id_2",
        "claim_approved_amount",
        "claim_approved_amount_per_premium_adj",
        F.greatest(F.col("premium_adj"), F.col("claim_approved_amount")).alias("greatest_premium_adj_claim_approved_amount"),
        F.round(F.col("claim_approved_amount") / F.nullif(F.col("premium_adj"), F.lit(0)), 3).alias("claim_approved_amount_per_premium_adj_2"),
    )
    .orderBy(F.col("claim_approved_amount_per_premium_adj").desc())
)
