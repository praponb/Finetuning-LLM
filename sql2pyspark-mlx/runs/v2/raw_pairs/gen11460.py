from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
)
