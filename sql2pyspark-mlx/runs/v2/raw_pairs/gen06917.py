from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([1, 2, 4]) & (F.col("ss.revenue") >= 1500))
    .select(F.col("ss.units"), F.col("s.opened_date"), F.col("s.sqft"), F.col("s.city"))
    .orderBy(F.col("sqft").desc(), "opened_date")
)
