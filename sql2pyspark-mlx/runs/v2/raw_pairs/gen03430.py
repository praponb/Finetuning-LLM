from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.sale_date").between("2023-06-01", "2024-04-01"))
    .groupBy(
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"),
        F.col("ss.product_id"),
    )
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
)
