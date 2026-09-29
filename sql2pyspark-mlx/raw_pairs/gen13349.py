from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").isNotNull())
    .groupBy(F.col("p.category"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("min_last_restock")
)
