from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.id"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc(), F.col("inventory_count").desc(), "id")
    .limit(20)
)
