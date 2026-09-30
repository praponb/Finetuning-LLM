from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 34) & F.col("email").like("%@company.co"))
    .groupBy(
        "level",
        F.when(F.col("job_title").isin(["Director", "Analyst"]), "priority").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.count("*").alias("employee_count"),
        F.max("hire_date").alias("last_hire_date"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
