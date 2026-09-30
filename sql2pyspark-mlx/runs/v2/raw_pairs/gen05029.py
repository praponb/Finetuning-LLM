from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.sale_date") < "2025-06-11") & (F.col("i.quantity") != 87))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.product_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.revenue"),
        F.date_trunc("year", F.col("i.last_restock")).alias("last_restock_period"),
    )
)
