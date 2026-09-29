from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("stock_qty")).otherwise(0)).alias("grocery_stock_qty"),
        F.min("price").alias("min_price"),
    )
    .orderBy("min_price", F.col("grocery_stock_qty").desc(), "category", "brand")
    .limit(10)
)
