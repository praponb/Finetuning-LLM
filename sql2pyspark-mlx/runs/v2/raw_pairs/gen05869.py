from pyspark.sql import functions as F

min_units = df_store_sales.agg(F.min("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") <= min_units)
    .select("id", "units")
    .orderBy("id", "units")
)
