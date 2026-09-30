from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("holder_id") >= 5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.when(F.col("premium") >= 2945.9, "A").when(F.col("premium") >= 2096.6, "B").otherwise("C").alias("premium_tier"),
    )
)

status_stats = (
    cleaned_policys
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    cleaned_policys.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

tagged = (
    outliers
    .filter(F.col("avg_premium").between(1160.0, 3320.0))
    .select(
        "id",
        "product",
        "start_date",
        "premium_tier",
        "avg_premium",
        "premium_minus_avg",
        F.coalesce(F.col("avg_premium"), F.lit(0.0)).alias("avg_premium_filled"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        (F.col("premium_minus_avg") - F.col("avg_premium")).alias("premium_minus_avg_minus_avg_premium"),
    )
)

result = (
    tagged
    .groupingSets(
        [["product", "product_or_unknown", "premium_tier"], ["product"], []],
        "product", "product_or_unknown", "premium_tier",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("product_or_unknown"), F.lit("ALL")).alias("product_or_unknown"),
        F.coalesce(F.col("premium_tier"), F.lit("ALL")).alias("premium_tier"),
        "grouping_level",
        "row_count",
    )
)
