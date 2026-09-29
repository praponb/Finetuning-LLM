from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.pct_analyst") >= 58)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("t.pct_analyst"),
        F.col("t.first_hire_date"),
    )
    .orderBy(F.col("budget").desc(), "pct_analyst")
)
