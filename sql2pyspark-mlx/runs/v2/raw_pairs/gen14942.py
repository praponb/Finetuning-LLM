from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.col("e.hire_date") > "2024-04-19") & F.col("e.job_title").isin(["Intern", "Analyst"]))
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("e.is_remote"),
        F.col("e.hire_date"),
    )
)
