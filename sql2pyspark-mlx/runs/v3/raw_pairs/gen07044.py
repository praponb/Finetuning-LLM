from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("s.region"),
        F.coalesce(F.col("s.region"), F.lit("Unknown")).alias("region_filled"),
    )
)
