from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.units") <= 39)
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.round((F.col("i.quantity") * 100), 2).alias("quantity_scaled"),
    )
)
