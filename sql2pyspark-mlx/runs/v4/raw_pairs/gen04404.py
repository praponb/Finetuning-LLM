from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.store_name").like("M%") & (F.col("s.sqft") > 8426))
    .select(F.col("ss.id"), F.col("ss.store_id"), F.col("s.opened_date"))
)
