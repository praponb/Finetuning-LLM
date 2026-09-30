from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("country").alias("label"))
    .unionByName(df_departments.select(F.col("location").alias("label")))
    .distinct()
)
