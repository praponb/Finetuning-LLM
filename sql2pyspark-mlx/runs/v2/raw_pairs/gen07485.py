from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.store_name").like("%Riversid%"))
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
    )
)
