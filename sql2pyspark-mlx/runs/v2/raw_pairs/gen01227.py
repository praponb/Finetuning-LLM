from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 4116000, "high").when(F.col("d.budget") >= 1998000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(
        F.sum(F.when(F.col("e.job_title") == "Analyst", F.col("e.salary")).otherwise(0)).alias("analyst_salary"),
    )
)
