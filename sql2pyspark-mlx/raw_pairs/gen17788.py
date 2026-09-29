from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("department_stats.first_hire_date"),
        F.col("department_stats.manager_count"),
    )
    .orderBy("manager_count")
)
