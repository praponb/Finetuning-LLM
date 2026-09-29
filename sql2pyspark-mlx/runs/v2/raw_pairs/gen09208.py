from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6, 9]))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 100), 2))
)

inventory_stats = (
    valid_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("quantity_scaled").alias("quantity_scaled_count"),
    )
)

result = (
    inventory_stats
    .filter(F.col("quantity_scaled_count") < 7)
    .orderBy(F.col("product_id").desc())
)
