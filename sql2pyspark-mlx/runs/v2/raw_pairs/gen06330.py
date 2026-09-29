from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("email").isNotNull() & F.col("job_title").isin(["Director", "Intern", "Analyst"]))
)

employees_stats = (
    recent_employees
    .groupBy("level")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    employees_stats
    .select("level", "total_salary", "max_salary")
    .orderBy(F.col("max_salary").desc())
)
