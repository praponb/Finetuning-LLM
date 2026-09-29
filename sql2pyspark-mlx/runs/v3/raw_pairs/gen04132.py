from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
