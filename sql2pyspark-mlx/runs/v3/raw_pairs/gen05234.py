from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("p.brand"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), "brand")
    .limit(50)
)
