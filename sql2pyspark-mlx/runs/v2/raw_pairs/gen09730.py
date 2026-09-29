from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.brand") == "Globex")
    .groupBy(
        F.when(F.col("products.stock_qty") >= 369, "high").when(F.col("products.stock_qty") >= 161, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.col("products.brand"),
    )
    .agg(
        F.max(F.col("inventory.last_restock")).alias("latest_last_restock"),
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)
