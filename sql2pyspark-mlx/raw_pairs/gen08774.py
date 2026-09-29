from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.revenue").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
