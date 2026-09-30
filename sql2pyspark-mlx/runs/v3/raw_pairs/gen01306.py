from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 17884, "high").when(F.col("s.sqft") >= 6820, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
)
