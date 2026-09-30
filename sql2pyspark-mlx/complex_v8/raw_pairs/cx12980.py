from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "TRAVEL"), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_stats = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

totals = (
    with_stats
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "product",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.sum(F.when(F.col("status").isNotNull(), F.col("premium"))).alias("premium_status_is_not_null"),
        F.min("start_date").alias("first_start_date"),
        F.min("premium").alias("min_premium"),
        F.max("first_claim_reported_at").alias("last_first_claim_reported_at"),
    )
    .filter(F.col("row_count") >= 2)
)

annotated = (
    totals
    .select(
        "*",
        F.when(F.col("avg_premium") >= 3560.0, "A").when(F.col("avg_premium") >= 1880.0, "B").otherwise("C").alias("avg_premium_tier"),
    )
)

result = (
    annotated
    .select(
        "start_date_month",
        "product",
        "row_count",
        "avg_premium",
        "premium_status_is_not_null",
        "first_start_date",
        "min_premium",
        "last_first_claim_reported_at",
        "avg_premium_tier",
        F.round(F.col("avg_premium") * 1.07, 2).alias("avg_premium_adj"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
    .orderBy(
        F.col("product").desc_nulls_first(),
        F.col("avg_premium_adj").asc_nulls_last(),
    )
)
