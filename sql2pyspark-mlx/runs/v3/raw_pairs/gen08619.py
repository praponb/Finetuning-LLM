from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("ss.sale_date")).alias("first_sale_date"))
    .orderBy(F.col("city").desc(), "first_sale_date")
)
