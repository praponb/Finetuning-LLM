from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("last_restock") <= "2025-04-01")

summary = (
    recent_inventory
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    summary
    .orderBy("last_restock_month", "store_id")
    .limit(5)
)
