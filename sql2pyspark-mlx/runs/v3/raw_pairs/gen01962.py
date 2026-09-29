from pyspark.sql import functions as F

valid_products = df_products.filter(F.year("launch_date") >= 2025)

agg = (
    valid_products
    .groupBy(
        F.when(F.col("cost") >= 640, "high").when(F.col("cost") >= 260, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = (
    agg
    .filter(F.col("total_stock_qty") >= 1700)
    .select("cost_bucket", "total_stock_qty")
    .orderBy("cost_bucket")
)
