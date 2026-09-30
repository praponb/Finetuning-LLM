from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 6]) & (F.col("last_restock") < "2023-08-01"))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)

product_id_summary = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("days_since_last_restock").alias("unique_products"),
        F.min("days_since_last_restock").alias("min_days_since_last_restock"),
    )
)

result = (
    product_id_summary
    .select("product_id", "store_id", "unique_products", "min_days_since_last_restock")
    .orderBy("store_id", "product_id")
    .limit(10)
)
