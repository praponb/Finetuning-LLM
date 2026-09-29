from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("inventory.quantity") <= 223)
    .groupBy(
        F.when(F.col("products.stock_qty") >= 366, "high").when(F.col("products.stock_qty") >= 138, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.count(F.col("inventory.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
)
