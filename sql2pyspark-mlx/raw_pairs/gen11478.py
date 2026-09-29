from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.units") == 32) & F.col("ss.store_id").isin([1, 3]))
    .select(F.col("i.quantity"), F.col("ss.store_id"), F.col("ss.revenue"))
    .orderBy("quantity")
)
