from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.quantity") <= 71) & F.col("ss.product_id").isin([6, 9]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.units"),
        F.coalesce(F.col("i.quantity"), F.lit(0)).alias("quantity_filled"),
    )
)
