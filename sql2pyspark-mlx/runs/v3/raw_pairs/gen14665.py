from pyspark.sql import functions as F

max_units = df_store_sales.agg(F.max("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= max_units)
    .select("id", "units")
    .orderBy(F.col("units").desc(), F.col("id").desc())
    .limit(5)
)
