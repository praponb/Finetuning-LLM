from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.units") <= 20) & (F.col("i.store_id") == 3))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.datediff(F.current_date(), F.col("i.last_restock")).alias("days_since_last_restock"),
    )
    .orderBy("last_restock", F.col("days_since_last_restock").desc())
)
