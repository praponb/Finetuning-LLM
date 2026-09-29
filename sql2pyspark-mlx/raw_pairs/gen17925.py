from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("avg_quantity") >= 230)
    .orderBy("avg_quantity")
)
