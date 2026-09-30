from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "brand")
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.min("launch_date").alias("min_launch_date"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
    .orderBy("umbrella_count", "supplier_id", "brand")
    .limit(20)
)
