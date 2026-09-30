from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.sqft"), F.col("s.opened_date"))
)
