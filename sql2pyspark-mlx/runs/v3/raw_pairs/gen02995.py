from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "right")
    .filter(F.col("departments.budget").between(3465000, 3626000))
    .groupBy(
        F.when(F.col("departments.budget") >= 3352000, "high").when(F.col("departments.budget") >= 779000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("departments.dept_name").isin(["Marketing", "Engineering"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.min(F.col("employees.salary")).alias("min_salary"),
        (F.max(F.col("employees.salary")) - F.min(F.col("employees.salary"))).alias("salary_range"),
    )
    .orderBy("budget_bucket", "salary_range")
)
