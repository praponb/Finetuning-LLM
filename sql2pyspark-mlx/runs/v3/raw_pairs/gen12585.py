from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft").between(10603, 15956)).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.store_id") == 2)
    .select(F.col("ss.id"), F.col("ss.units"), F.col("ss.sale_date"))
    .orderBy(F.col("units").desc(), "id")
    .limit(5)
)
