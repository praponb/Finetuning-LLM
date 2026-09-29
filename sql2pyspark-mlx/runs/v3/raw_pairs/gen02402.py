from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.quantity") > 109)
    .groupBy(F.date_trunc("MONTH", F.col("store_sales.sale_date")).alias("sale_month"))
    .agg(F.min(F.col("inventory.last_restock")).alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc())
)
