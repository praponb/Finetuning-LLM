from pyspark.sql import functions as F

avg_units = df_store_sales.agg(F.avg("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= avg_units)
    .select("id", "units")
    .orderBy(F.col("units").desc())
)
