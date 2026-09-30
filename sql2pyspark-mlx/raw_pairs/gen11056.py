from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("store_name").like("%Cent%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.month(F.col("ss.sale_date")).isin([3, 4, 12]))
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("ss.store_id"))
)
