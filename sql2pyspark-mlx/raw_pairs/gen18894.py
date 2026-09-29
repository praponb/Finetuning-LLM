from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.cost").isNotNull())
    .groupBy(
        F.when(F.col("p.stock_qty") >= 318, "high").when(F.col("p.stock_qty") >= 194, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("stock_qty_bucket").desc())
    .limit(10)
)
