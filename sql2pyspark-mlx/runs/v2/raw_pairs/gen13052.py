from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 12692, "high").when(F.col("s.sqft") >= 2830, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 190)
)
