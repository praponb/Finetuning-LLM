from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("revenue") <= 2900).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.city").isin(["Berlin", "Bangkok", "London"]))
    .select(F.col("s.id"), F.col("s.region"), F.col("s.opened_date"), F.col("s.sqft"))
    .orderBy(F.col("sqft").desc())
)
