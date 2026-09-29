from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-11-20").cast("date")) & (~F.col("status").isin("EXPIRED", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
    )
)

with_children_claims = (
    prepared.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

without_matches = (
    with_children_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").isNotNull()), "left_anti")
)

detail_rows = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.claim_id",
        "s.claim_claimed_amount",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "holder_id",
        "claim_claimed_amount",
        "claim_id_2",
        "claim_date",
        "claim_claimed_amount_2",
        F.when(F.col("claim_claimed_amount") >= 32878.5, "HIGH").when(F.col("claim_claimed_amount") >= 19337.6, "MEDIUM").otherwise("LOW").alias("claim_claimed_amount_tier"),
    )
)
