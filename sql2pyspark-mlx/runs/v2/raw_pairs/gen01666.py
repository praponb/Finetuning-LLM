from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.quantity") < 141) | F.col("ss.sale_date").between("2025-04-01", "2025-07-01"))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
)
