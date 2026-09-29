from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .filter(F.col("inventory_count") > 1)
    .orderBy(F.col("unique_stores").desc(), "brand")
)
