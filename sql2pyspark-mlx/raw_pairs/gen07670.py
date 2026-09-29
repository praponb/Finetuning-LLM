from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([3, 9, 10]))
    .select(F.col("ss.product_id"), F.col("s.opened_date"))
    .orderBy("product_id")
)
