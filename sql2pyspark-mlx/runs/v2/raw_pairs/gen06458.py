from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .rollup(
        F.when(F.col("quantity") >= 213, "high").when(F.col("quantity") >= 69, "mid").otherwise("low").alias("quantity_bucket"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.count("*").alias("inventory_count"),
    )
    .orderBy("inventory_count")
)
