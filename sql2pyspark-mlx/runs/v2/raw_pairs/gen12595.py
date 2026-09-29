from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") >= 1249000)
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.budget") >= 3260000, "high").when(F.col("d.budget") >= 1030000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.col("e.salary")).alias("total_salary"),
        F.min(F.col("e.salary")).alias("min_salary"),
    )
)
