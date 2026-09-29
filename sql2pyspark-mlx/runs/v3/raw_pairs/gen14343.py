from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([1, 6]) | F.col("i.last_restock").between("2023-10-19", "2023-11-01"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("ss.store_id").alias("store_sale_store_id"),
    )
)
