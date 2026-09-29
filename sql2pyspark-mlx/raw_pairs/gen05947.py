from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.salary_range") >= 36)
    .select(F.col("d.id"), F.col("d.location"), F.col("ds.salary_range"))
    .orderBy("id")
)
