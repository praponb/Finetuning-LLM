from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("s.sqft") != 9448)
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
)
