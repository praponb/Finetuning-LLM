from pyspark.sql import functions as F

avg_units = df_store_sales.filter(F.col("revenue") <= 3700).agg(F.avg("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= avg_units)
    .select("id", "units", "store_id")
    .orderBy(F.col("id").desc())
)
