from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.when(F.col("d.budget") >= 3944000, "high").when(F.col("d.budget") >= 782000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"))
    .orderBy("avg_salary")
)
