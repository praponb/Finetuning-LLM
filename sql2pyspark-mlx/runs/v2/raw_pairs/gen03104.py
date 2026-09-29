from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 780, "high").when(F.col("cost") >= 200, "mid").otherwise("low").alias("cost_bucket"),
        F.when(F.col("stock_qty") >= 359, "high").when(F.col("stock_qty") >= 139, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.min("launch_date").alias("first_launch_date"))
)
