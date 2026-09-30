from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 460) & (F.col("stock_qty") > 206))
    .groupBy("category")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy(F.col("total_stock_qty").desc(), F.col("cost_range").desc(), "category")
    .limit(20)
)
