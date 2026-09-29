from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.product_id") == 5) & (F.col("ss.sale_date") <= "2024-12-01"))
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.units"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("i.quantity").cast("string").alias("quantity_str"),
    )
)
