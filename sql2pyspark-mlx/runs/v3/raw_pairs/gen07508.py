from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.upper(F.col("s.store_name")).like("%LD %"))
    .select(F.col("ss.sale_date"), F.col("s.store_name"), F.col("s.city"))
)
