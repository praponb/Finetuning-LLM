from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("launch_date") > "2024-11-14") | (F.year("launch_date") == 2024))
)

products_stats = (
    recent_products
    .groupBy(
        F.when(F.col("stock_qty") >= 354, "high").when(F.col("stock_qty") >= 142, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.countDistinct("price").alias("unique_brands"))
)

result = products_stats.orderBy("stock_qty_bucket")
