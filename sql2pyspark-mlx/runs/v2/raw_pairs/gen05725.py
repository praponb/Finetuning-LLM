from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance") & (F.col("supplier_id") == 31) & (F.size("tags") > 2))
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
    )
)
