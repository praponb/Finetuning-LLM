from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id").isin([5, 8]))

summary = (
    recent_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .filter(F.col("total_quantity") >= 810)
    .select("product_id", "total_count")
)
