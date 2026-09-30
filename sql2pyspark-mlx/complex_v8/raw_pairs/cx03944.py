from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

labeled = (
    covered
    .select(
        "*",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.abs(F.col("premium") - 2945.9).alias("premium_distance"),
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "adjuster_id",
        "claim_type",
        "product",
        "premium",
        "product_len",
        "premium_distance",
        "claim_date_month",
        F.when(F.col("claimed_amount") >= 29985.9, "large").when(F.col("claimed_amount") >= 7204.1, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.concat(F.col("product"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("product_ref"),
    )
    .orderBy(F.col("premium").asc(), F.col("claim_type").desc())
)
