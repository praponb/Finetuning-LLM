from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("store_id") == 1) & (F.col("last_restock") >= "2023-10-01"))
)

store_id_summary = (
    recent_inventory
    .groupBy("store_id", "product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    store_id_summary
    .filter(F.col("quantity_range") > 61)
    .select("store_id", "product_id")
    .orderBy("product_id", "store_id")
)
