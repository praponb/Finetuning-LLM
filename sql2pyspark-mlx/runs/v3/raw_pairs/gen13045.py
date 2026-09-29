from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "right")
    .groupBy(
        F.col("departments.location"),
        F.when(F.col("departments.budget") >= 3462000, "high").when(F.col("departments.budget") >= 2029000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("employees.job_title")))).alias("job_title_values"),
    )
    .orderBy("location", "job_title_values")
)
