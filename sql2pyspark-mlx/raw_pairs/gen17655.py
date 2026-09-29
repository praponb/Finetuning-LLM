from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Mia%"))
    .select("job_title", F.lit("employee").alias("kind"))
    .union(
        df_customers
        .filter(F.col("name").like("Pim%"))
        .select("city", F.lit("customer").alias("kind"))
    )
)
