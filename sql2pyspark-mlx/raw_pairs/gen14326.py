from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .filter(F.col("department_stats.max_salary") >= 57000)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("department_stats.stddev_salary"),
        F.col("department_stats.max_salary"),
    )
    .orderBy("stddev_salary", "id")
    .limit(5)
)
