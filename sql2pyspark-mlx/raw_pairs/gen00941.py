from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.last_restock").between("2023-09-01", "2025-03-28"))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("ss.store_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.units"),
        F.round((F.col("i.quantity") * 1.15), 2).alias("quantity_adjusted"),
    )
    .orderBy("last_restock")
)
