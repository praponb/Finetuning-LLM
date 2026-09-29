from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(F.col("ss.product_id"), F.col("s.city"), F.col("s.opened_date"))
    .orderBy("opened_date")
)
