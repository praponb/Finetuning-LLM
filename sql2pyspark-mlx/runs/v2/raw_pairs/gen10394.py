from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.month(F.col("i.last_restock")).isin([5, 9, 10]) & (F.col("ss.units") <= 11))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("id").desc())
)
