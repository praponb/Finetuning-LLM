from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 140)
    .select("store_id", "last_restock_month", "max_quantity")
    .orderBy("max_quantity", "store_id", "last_restock_month")
    .limit(1)
)
