from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 344, "high").when(F.col("stock_qty") >= 115, "mid").otherwise("low").alias("stock_qty_bucket"),
        "category",
    )
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)
