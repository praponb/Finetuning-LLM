from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("departments.dept_name").isin(["Marketing", "Finance"]))
    .groupBy(
        F.when(F.col("departments.budget") >= 3508000, "high").when(F.col("departments.budget") >= 1855000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 2)
    .orderBy("budget_bucket")
    .limit(5)
)
