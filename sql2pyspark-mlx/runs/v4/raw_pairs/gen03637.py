from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("launch_date") <= "2024-06-06") & (F.col("cost").between(450, 560) | (F.col("cost") <= 360)))
    .withColumn("product_name_lower", F.lower("product_name"))
)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0)).alias("electronics_count"),
        F.sum("cost").alias("total_cost"),
    )
)

result = products_stats.select("category", "electronics_count", "total_cost")
