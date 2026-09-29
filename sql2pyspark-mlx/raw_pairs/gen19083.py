from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.when(F.col("s.region").isin(["South", "North"]), "priority").otherwise("other").alias("region_group"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("region_group").desc(), "stddev_quantity")
)
