from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.revenue") >= 1200) | F.col("ss.product_id").isin([4, 6, 9, 10]))
    .select(
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.round(F.col("i.quantity"), 2).alias("quantity_rounded"),
    )
)
