from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.avg("price").alias("avg_price"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
)
