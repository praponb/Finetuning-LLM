from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select("full_name")
    .unionByName(
        df_customers
        .filter(F.col("age") < 59)
        .select(F.col("name").alias("full_name"))
    )
    .distinct()
    .orderBy(F.col("full_name").desc())
)
