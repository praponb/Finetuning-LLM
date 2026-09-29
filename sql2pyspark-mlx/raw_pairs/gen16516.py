from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.lower("store_name").like("%iam%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("ss.sale_date"))
    .orderBy(F.col("store_id").desc())
)
