from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
)
