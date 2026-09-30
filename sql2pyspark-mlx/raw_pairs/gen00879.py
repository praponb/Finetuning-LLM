from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(F.col("s.sqft"), F.col("s.city"), F.col("ss.sale_date"))
)
