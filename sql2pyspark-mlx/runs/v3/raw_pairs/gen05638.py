from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.length(F.col("s.store_name")) >= 9)
    .select(
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("city").desc())
)
