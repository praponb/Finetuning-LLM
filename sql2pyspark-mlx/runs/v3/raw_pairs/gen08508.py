from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 4, 10]) & F.col("supplier_id").isNull() & (F.length("product_name") < 11))
    .groupBy(
        "category",
        F.when(F.col("price") >= 1080, "high").when(F.col("price") >= 170, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)
