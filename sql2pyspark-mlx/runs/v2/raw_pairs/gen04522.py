from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.min("price").alias("min_price"),
        F.sum("cost").alias("total_cost"),
    )
)
