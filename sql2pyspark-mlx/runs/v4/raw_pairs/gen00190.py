from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.month(F.col("i.last_restock")).isin([2, 4, 9]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
    .orderBy(F.col("store_id").desc())
)
