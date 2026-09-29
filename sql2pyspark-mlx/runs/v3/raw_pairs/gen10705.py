from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.sale_date") < "2024-06-01") & (F.col("ss.units") == 25))
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
    )
)
