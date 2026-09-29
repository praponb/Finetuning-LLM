from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("p.product_name").isNotNull() & F.col("s.region").isNull())
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.round(F.col("ss.revenue"), 2).alias("revenue_rounded"),
    )
    .orderBy("revenue_rounded")
)
