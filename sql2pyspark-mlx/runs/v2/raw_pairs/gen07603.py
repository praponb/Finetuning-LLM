from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(~F.col("store_name").like("%Old%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.units") == 23)
    .select(F.col("ss.id"), F.col("ss.revenue"))
)
