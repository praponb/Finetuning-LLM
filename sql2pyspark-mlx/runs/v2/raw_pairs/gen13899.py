from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.length(F.col("s.store_name")) > 9)
    .select(
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("s.region"),
        (F.col("ss.revenue") * F.col("ss.units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("region").desc())
)
