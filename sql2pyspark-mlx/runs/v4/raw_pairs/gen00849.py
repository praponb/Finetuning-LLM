from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.quantity") != 68)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.revenue"),
        F.date_add(F.col("i.last_restock"), 72).alias("last_restock_plus_72d"),
    )
)
