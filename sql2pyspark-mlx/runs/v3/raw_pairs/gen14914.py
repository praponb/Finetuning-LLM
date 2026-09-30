from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 160)
    .select("store_id", "last_restock_month", "avg_quantity")
)
