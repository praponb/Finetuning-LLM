from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2025-08-08") & F.col("quantity").between(94, 134))
    .groupBy("product_id", "store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)
