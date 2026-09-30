from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.store_id") == 1) & F.col("ss.sale_date").between("2023-06-25", "2025-01-06"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("ss.product_id"),
    )
    .orderBy(F.col("quantity").desc())
)
