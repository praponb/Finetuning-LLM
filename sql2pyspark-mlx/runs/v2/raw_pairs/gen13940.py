from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Marketing", "Legal"]))
    .groupBy(
        F.when(F.col("d.budget") >= 3067000, "high").when(F.col("d.budget") >= 1964000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(F.count(F.when(F.col("e.job_title") == "Director", 1)).alias("director_count"))
)
