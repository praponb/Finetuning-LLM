from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .filter(F.col("department_stats.max_salary") > 67000)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("department_stats.max_salary"),
    )
    .orderBy(F.col("dept_name").desc())
)
