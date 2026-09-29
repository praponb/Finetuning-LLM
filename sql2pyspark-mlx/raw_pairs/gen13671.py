from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "SG")
    .select(F.col("segment").alias("label"))
    .subtract(df_employees.select(F.col("job_title").alias("label")))
    .orderBy("label")
)
