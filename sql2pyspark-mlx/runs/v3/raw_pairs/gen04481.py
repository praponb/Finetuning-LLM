from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-03-01")
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .orderBy("first_last_restock", "last_restock_month", "store_id")
    .limit(50)
)
