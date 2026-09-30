from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.year(F.col("ss.sale_date")) == 2025)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("product_id").desc())
)
