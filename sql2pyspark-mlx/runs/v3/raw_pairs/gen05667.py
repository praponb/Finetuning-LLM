from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4]) & F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "id",
        "quantity",
        "store_id",
        F.date_add("last_restock", 53).alias("last_restock_plus_53d"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
    .orderBy("last_restock_label", "id")
    .limit(1)
)
