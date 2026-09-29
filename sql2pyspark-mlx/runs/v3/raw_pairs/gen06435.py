from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 680, "high").when(F.col("cost") >= 140, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("*").alias("product_count"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .filter(F.col("product_count") > 1)
    .orderBy(F.col("product_count").desc())
)
