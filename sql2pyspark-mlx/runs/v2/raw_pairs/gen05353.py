from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Bac%"))
    .groupBy("category")
    .agg(
        F.sum("price").alias("total_price"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("stock_qty")).otherwise(0)).alias("stark_stock_qty"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
)
