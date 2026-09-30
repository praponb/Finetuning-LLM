from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 2)
    .orderBy(F.col("store_sale_count").desc(), "opened_month")
)
