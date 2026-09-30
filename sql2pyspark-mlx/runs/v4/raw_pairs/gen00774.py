from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("i.product_id").isin([4, 8]))
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.brand").isin(["Initech", "Globex"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
)
