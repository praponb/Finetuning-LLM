from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("kind"))
    .unionByName(
        df_employees
        .filter(F.col("job_title").isin(["Designer", "Intern", "Analyst"]))
        .select("email", F.lit("employee").alias("kind"))
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
