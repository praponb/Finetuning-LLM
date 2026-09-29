from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") < 79000)
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.max_salary") > 75000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("t.avg_salary"),
        F.col("t.max_salary"),
    )
)
