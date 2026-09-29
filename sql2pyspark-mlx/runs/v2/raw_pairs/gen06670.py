from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("job_title") == "Engineer")

level_summary = (
    valid_employees
    .groupBy("level")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = level_summary.select("level", "avg_salary")
