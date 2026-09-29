from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.lower(F.trim("email")) == "emma@outlook.com")
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.salary_range") > 49)
    .select(F.col("d.id"), F.col("d.budget"), F.col("ds.salary_range"))
)
