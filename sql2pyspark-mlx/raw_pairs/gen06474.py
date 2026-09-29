from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter((F.col("store_id") == 6) & (F.col("last_restock") > "2023-04-01"))
)

inventory_stats = (
    filtered_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 199, "high").when(F.col("quantity") >= 107, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    inventory_stats
    .filter(F.col("quantity_range") >= 52)
    .select("store_id", "quantity_bucket", "quantity_range")
    .orderBy("quantity_bucket")
)
