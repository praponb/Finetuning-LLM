from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 315, "high").when(F.col("stock_qty") >= 107, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
    .filter(F.col("total_stock_qty") < 2100)
)
