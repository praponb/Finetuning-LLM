from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost").isNull())
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.sqft") >= 17575, "high").when(F.col("s.sqft") >= 3177, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc(), "region", "sqft_bucket")
    .limit(1)
)
