from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.product_name").like("Lap%"))
    .groupBy(F.col("p.category"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
    )
)
