from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1340, "high").when(F.col("price") >= 510, "mid").otherwise("low").alias("price_bucket"),
        "category",
        "brand",
    )
    .agg(
        F.avg("price").alias("avg_price"),
        F.min("launch_date").alias("min_launch_date"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
