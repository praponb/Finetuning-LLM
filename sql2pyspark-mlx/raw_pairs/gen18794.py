from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)
