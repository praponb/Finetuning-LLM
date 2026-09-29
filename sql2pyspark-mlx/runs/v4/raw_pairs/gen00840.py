from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([6, 7, 10]))
    .select(F.col("s.opened_date"), F.col("ss.store_id"), F.col("ss.product_id"))
    .orderBy("store_id", "opened_date")
)
