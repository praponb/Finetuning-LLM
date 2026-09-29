from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(F.col("ss.units"), F.col("s.sqft"))
    .orderBy(F.col("sqft").desc(), "units")
)
