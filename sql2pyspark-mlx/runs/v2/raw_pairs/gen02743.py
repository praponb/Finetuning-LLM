from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id").isin([5, 6]) & F.col("ss.product_id").isin([2, 6, 8, 10]))
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
    )
    .orderBy("store_id")
)
