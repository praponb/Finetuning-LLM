from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.product_id") == 5)
    .select(
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
    )
)
