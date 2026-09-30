from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([3, 4, 8]) | (F.col("ss.sale_date") < "2023-03-01"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
    )
    .orderBy("quantity", F.col("id").desc())
)
