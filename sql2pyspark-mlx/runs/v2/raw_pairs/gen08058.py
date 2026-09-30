from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.col("employees.is_remote"))
    .groupBy(
        F.when(F.col("departments.budget") >= 3835000, "high").when(F.col("departments.budget") >= 1448000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("departments.location"),
    )
    .agg(F.sum(F.col("employees.salary")).alias("total_salary"))
    .filter(F.col("total_salary") > 530000)
)
