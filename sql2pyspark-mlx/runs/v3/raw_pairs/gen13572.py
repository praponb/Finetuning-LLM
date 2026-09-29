from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity") <= 138)
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
)

inventory_stats = (
    recent_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "store_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    inventory_stats
    .select("last_restock_cohort", "store_id", "avg_quantity", "unique_products")
    .orderBy(F.col("avg_quantity").desc())
)
