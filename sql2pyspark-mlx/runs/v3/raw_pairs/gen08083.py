from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("i.product_id") == 1) & (F.col("i.quantity") < 149))
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.units"),
        F.coalesce(F.col("i.quantity"), F.lit(0)).alias("quantity_filled"),
    )
)
