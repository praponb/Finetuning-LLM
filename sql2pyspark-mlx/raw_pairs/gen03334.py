from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.quantity").between(95, 136) & F.col("ss.product_id").isin([1, 4, 8]))
    .select(F.col("i.quantity"), F.col("ss.product_id"), F.col("ss.store_id"))
)
