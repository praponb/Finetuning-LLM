from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 359, "high").when(F.col("stock_qty") >= 116, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .orderBy("stock_qty_bucket")
)
