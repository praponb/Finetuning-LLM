from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("category")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum("price").alias("total_price"),
    )
    .filter(F.col("total_price") > 5500)
    .drop("total_price")
    .orderBy(F.col("avg_cost").desc(), F.col("category").desc())
)
