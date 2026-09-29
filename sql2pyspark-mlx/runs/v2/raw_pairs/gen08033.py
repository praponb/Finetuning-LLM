from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
    .orderBy(F.col("max_stock_qty").desc(), "brand")
    .limit(10)
)
