from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([2, 3, 8, 10]) & F.col("i.store_id").isin([5, 6]))
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.units"),
    )
    .orderBy(F.col("units").desc())
)
