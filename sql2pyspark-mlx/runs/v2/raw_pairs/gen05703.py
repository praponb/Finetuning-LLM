from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.last_restock") <= "2025-10-01")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("ss.units"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
)
