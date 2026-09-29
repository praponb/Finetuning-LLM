from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.category") == "Home")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
)
