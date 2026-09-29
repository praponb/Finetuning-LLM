from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("hire_date") < "2025-08-02") & F.col("full_name").like("%MILLER%") & F.col("job_title").isin(["Director", "Analyst"]))
)

employees_stats = (
    filtered_employees
    .groupBy("is_remote", "level")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    employees_stats
    .select("is_remote", "level")
    .orderBy("level")
)
