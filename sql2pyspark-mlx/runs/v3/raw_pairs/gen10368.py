from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 4, 6]))
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.region").isin(["West", "North"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("units_count")
)
