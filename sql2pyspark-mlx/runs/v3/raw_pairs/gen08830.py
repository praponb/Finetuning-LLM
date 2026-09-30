from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(df_departments.select(F.col("location").alias("city")))
    .orderBy("city")
)
