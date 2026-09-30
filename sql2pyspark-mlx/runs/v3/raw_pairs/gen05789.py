from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
    .orderBy(F.col("stock_qty_count").desc())
)
