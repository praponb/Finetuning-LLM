from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units").between(16, 21))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.product_id").alias("store_sale_product_id"),
    )
    .orderBy("product_id")
)
