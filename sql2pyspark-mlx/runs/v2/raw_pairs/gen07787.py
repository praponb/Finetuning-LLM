from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([1, 4]) & (F.col("ss.units") != 19))
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.units"),
    )
)
