from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.location") != "London")
    .groupBy(
        F.when(F.col("d.budget") >= 3340000, "high").when(F.col("d.budget") >= 713000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg(F.col("e.salary")).alias("avg_salary"),
        F.sum(F.when(F.col("e.job_title") == "Manager", F.col("e.salary")).otherwise(0)).alias("manager_salary"),
    )
    .filter(F.col("manager_salary") >= 73)
    .orderBy("budget_bucket")
)
