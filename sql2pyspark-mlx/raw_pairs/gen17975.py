from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("name").alias("label"))
    .intersect(df_employees.select(F.col("full_name").alias("label")))
)
