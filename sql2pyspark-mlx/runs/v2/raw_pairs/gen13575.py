from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([2, 7, 8]))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.07), 2))
)

inventory_stats = (
    valid_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("avg_quantity") > 80)
    .orderBy(F.col("store_id").desc(), "last_restock_month")
    .limit(1)
)
