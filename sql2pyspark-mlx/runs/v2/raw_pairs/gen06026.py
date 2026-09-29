from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 4)
    .orderBy(F.col("unique_stores").desc())
)
