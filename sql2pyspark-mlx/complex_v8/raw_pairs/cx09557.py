from pyspark.sql import functions as F

base = df_policies.filter((F.col("product") != "HOME") & (F.year(F.col("end_date")) == 2026))

combined_details_holder = (
    base.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") != "RETAIL"), "inner")
    .filter(F.col("p.joined_on").between(F.lit("2024-02-04").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

enriched_holder = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.holder_region",
        "s.holder_joined_on",
        F.col("p.region").alias("holder_region_2"),
    )
)

labeled = (
    enriched_holder
    .select(
        "*",
        F.date_trunc("quarter", F.col("holder_joined_on")).alias("holder_joined_on_quarter"),
        F.substring(F.trim(F.col("holder_region")), 1, 3).alias("holder_region_prefix"),
    )
)

line_level = (
    labeled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.holder_region",
        "s.holder_joined_on",
        "s.holder_region_2",
        "s.holder_joined_on_quarter",
        "s.holder_region_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "premium",
        "holder_region_2",
        "holder_joined_on_quarter",
        "holder_region_prefix",
        "claim_id",
        "claim_claimed_amount",
        F.round(F.col("claim_claimed_amount") * 0.25, 2).alias("claim_claimed_amount_adj"),
    )
    .orderBy("premium")
)
