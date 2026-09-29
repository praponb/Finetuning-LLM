from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([4, 5]) & F.col("ss.store_id").isin([2, 3, 4]))
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
    .orderBy(F.col("store_id").desc())
)
