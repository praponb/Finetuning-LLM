from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .filter(F.col("sports_count") < 34)
)
