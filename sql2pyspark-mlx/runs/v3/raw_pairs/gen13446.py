from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.product_id") == 5)
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("city")
)
