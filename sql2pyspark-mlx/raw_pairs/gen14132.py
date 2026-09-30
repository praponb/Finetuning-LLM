from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(5967, 15352) & F.month(F.col("s.opened_date")).isin([4, 8, 12]))
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("s.opened_date"))
)
