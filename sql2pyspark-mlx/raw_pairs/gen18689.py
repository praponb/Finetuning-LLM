from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(F.col("ss.id"), F.col("ss.sale_date"), F.col("ss.units"), F.col("s.region"))
)
