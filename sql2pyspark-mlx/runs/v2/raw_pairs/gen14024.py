from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("full_name").like("%Jac%"))

employees_stats = (
    valid_employees
    .groupBy(
        F.when(F.col("salary") >= 153000, "high").when(F.col("salary") >= 80000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    employees_stats
    .filter(F.col("manager_count") >= 42)
    .select("salary_bucket", "manager_count", "job_title_list")
)
