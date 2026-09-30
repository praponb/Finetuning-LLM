from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.col("i.quantity") > 94)
    .groupBy(
        F.when(F.col("s.sqft") >= 12809, "high").when(F.col("s.sqft") >= 6074, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
)
