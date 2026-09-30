from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Engineer", "Analyst"]))
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.location"), F.col("E.salary_range"))
    .orderBy(F.col("location").desc())
)
