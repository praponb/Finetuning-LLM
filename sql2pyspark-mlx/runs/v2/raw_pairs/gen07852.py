from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_suppliers"),
        F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0)).alias("grocery_count"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .orderBy("category")
)
