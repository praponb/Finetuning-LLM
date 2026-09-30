from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-01", "2025-02-28") & (F.col("product_id") == 9))
)

store_id_summary = (
    src
    .groupBy("store_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = store_id_summary
