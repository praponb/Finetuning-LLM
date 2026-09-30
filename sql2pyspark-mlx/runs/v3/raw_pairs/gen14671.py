from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 1350)
    .orderBy(F.col("total_quantity").desc(), "region")
    .limit(5)
)
