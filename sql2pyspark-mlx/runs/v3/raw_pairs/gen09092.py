from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units").between(13, 40))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
    .orderBy("city", F.col("sale_date").desc())
)
