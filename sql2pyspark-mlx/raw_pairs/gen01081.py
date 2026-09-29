from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .groupBy(
        F.col("p.brand"),
        F.when(F.col("p.category").isin(["Sports", "Home"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .filter(F.col("unique_stores") > 4)
    .orderBy(F.col("unique_stores").desc())
)
