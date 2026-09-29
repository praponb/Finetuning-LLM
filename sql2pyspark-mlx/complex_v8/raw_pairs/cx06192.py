from pyspark.sql import functions as F

matched = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

per_group = (
    matched
    .groupBy("product")
    .agg(
        F.max("premium").alias("max_premium"),
        F.count("*").alias("row_count"),
        F.max("claimed_amount").alias("max_claimed_amount_check"),
    )
    .filter(F.col("max_claimed_amount_check") > 10673.3)
    .drop("max_claimed_amount_check")
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    without_matches
    .select(
        "product",
        "max_premium",
        "row_count",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
    .orderBy(F.col("row_count").desc(), F.col("product_prefix").asc_nulls_last())
)
