from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .groupBy(
        F.col("departments.dept_name"),
        F.when(F.col("departments.budget") >= 4398000, "high").when(F.col("departments.budget") >= 625000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct(F.col("employees.salary")).alias("unique_depts"),
        (F.max(F.col("employees.salary")) - F.min(F.col("employees.salary"))).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc(), "dept_name", "budget_bucket")
    .limit(10)
)
