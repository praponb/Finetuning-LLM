from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("store_name").like("%Airport%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.year(F.col("ss.sale_date")) >= 2025)
    .select(F.col("ss.id"), F.col("ss.revenue"))
)
