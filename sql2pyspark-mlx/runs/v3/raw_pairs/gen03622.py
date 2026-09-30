from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 444, "high").when(F.col("p.stock_qty") >= 123, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("unique_products") >= 5)
    .orderBy(F.col("num_inventory").desc())
)
