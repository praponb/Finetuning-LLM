from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.product_id") == 9) | F.col("i.store_id").isin([2, 3, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
    )
    .orderBy(F.col("id").desc())
)
