from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("p.brand").isin(["Globex", "Umbrella"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("brand_group").desc(), F.col("stddev_revenue").desc())
    .limit(1)
)
