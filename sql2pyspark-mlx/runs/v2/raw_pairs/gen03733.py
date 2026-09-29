from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("full_name").like("O%"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .filter(F.col("department_stats.l4_salary") > 36)
    .select(
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("department_stats.l4_salary"),
    )
    .orderBy("location")
)
