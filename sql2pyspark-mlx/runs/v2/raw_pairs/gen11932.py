from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.revenue") < 2300)
    .select(F.col("s.city"), F.col("ss.sale_date"))
)
