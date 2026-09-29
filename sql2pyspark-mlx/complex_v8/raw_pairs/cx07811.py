from pyspark.sql import functions as F

recent = (
    df_policies
    .filter((F.col("holder_id") >= 14) | (F.col("product").isin("HEALTH", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

annotated = (
    recent
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "product_slug",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

scored = (
    annotated
    .filter(F.col("holder_id") >= 6)
    .select(
        "*",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket_2"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

tagged = (
    scored
    .filter(F.col("holder_id").isNotNull())
    .withColumn("days_since_start_date", F.datediff(F.lit("2025-12-31").cast("date"), F.col("start_date")))
)

screened = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id") >= 6) & (F.col("c.reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("premium") < 3544.1)
)

grouped = (
    screened
    .groupBy(
        "holder_id",
        "product",
        F.date_trunc("month", "end_date").alias("end_date_month"),
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.max("start_date").alias("last_start_date"),
        F.max("end_date").alias("last_end_date"),
        F.countDistinct("product").alias("distinct_products"),
        F.count("*").alias("row_count"),
        F.min("premium_bucket").alias("min_premium_bucket"),
    )
)

result = (
    grouped
    .select(
        "holder_id",
        "last_start_date",
        "distinct_products",
        "row_count",
        "min_premium_bucket",
    )
    .orderBy(F.col("holder_id").desc())
)
