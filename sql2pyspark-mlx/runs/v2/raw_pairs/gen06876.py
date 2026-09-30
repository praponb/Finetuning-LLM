from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 3129000, "high").when(F.col("d.budget") >= 668000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
    )
)
