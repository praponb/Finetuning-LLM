from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("opened_date") >= "2024-05-01").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("revenue").desc())
)
