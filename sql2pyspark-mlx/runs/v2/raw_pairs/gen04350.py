from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.upper(F.col("p.product_name")).like("%OIS%"))
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
)
