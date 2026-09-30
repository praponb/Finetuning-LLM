from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("p.brand").isin(["Stark", "Globex"]), "tier_1").otherwise("other").alias("brand_group"),
        F.col("p.brand"),
    )
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
