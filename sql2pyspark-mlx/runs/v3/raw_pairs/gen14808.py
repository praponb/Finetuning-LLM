from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(~(F.col("store_sales.product_id").isin([2, 9, 10]) | F.col("inventory.product_id").isin([4, 6, 7])))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
        F.col("store_sales.product_id"),
    )
    .orderBy(F.col("store_id").desc(), F.col("product_id").desc())
)
