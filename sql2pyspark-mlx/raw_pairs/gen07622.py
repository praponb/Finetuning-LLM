from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id").isin([3, 4, 7, 8])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.opened_date"))
)
