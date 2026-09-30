from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.when(F.col("p.cost") >= 700, "high").when(F.col("p.cost") >= 110, "mid").otherwise("low").alias("cost_bucket"),
        F.col("p.category"),
    )
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
)
