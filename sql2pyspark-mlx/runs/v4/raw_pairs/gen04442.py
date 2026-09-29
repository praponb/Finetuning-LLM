from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 430, "high").when(F.col("stock_qty") >= 141, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.count("stock_qty").alias("stock_qty_count"))
    .filter(F.col("stock_qty_count") >= 4)
)
