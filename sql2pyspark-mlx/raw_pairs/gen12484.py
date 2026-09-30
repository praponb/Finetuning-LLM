from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") < "2025-04-01") & F.col("product_name").like("%Lapt%"))
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
)
