from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([2, 5, 10]) & (F.col("i.quantity") <= 61))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.revenue").cast("int").alias("revenue_int"),
    )
    .orderBy(F.col("revenue_int").desc())
)
