from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.max("price").alias("max_price"),
    )
    .filter(F.col("fashion_count") < 9)
    .select("category", "fashion_count", "max_price")
)
