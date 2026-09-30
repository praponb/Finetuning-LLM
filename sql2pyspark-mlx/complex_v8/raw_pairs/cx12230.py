from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("premium") > 2111.3) | (F.col("start_date") >= F.lit("2024-06-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.when(F.col("product").isin("TRAVEL", "AUTO"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

derived = (
    recent_policys
    .select(
        "*",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

status_stats = (
    derived
    .groupBy("status")
    .agg(F.max("premium_bucket").alias("max_premium_bucket"))
)

vs_group = (
    derived.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium_bucket") == F.col("g.max_premium_bucket"))
    .select(
        "s.*",
        "g.max_premium_bucket",
        F.round(F.col("s.premium_bucket") - F.col("g.max_premium_bucket"), 2).alias("premium_bucket_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "product_group",
        "premium_bucket",
        "premium_filled",
        "max_premium_bucket",
        "premium_bucket_minus_max",
    )
    .orderBy(F.col("id").desc(), F.col("premium").desc())
)
