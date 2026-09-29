from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("p.supplier_id"),
        F.when(F.col("p.stock_qty") >= 413, "high").when(F.col("p.stock_qty") >= 194, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .filter(F.col("max_quantity") >= 90)
)
