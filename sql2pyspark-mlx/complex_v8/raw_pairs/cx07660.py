from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

tagged = (
    joined_ranges
    .select(
        "id",
        "claim_date",
        "status",
        "product",
        "premium",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.when(F.col("claim_type").isin("fire", "flood"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

result = (
    tagged
    .select(
        "id",
        "claim_date",
        "product",
        "premium",
        "claim_type_group",
        F.concat_ws("|", F.col("product"), F.col("claim_type_group")).alias("product_claim_type_group_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date_2"),
    )
    .orderBy(F.col("claim_type_group").asc(), F.col("claim_date").desc())
)
