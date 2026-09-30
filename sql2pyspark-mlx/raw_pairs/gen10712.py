from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 3907000, "high").when(F.col("d.budget") >= 1910000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("e.salary")), 2).alias("stddev_salary"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
    )
    .orderBy(F.col("salary_range").desc(), "budget_bucket")
    .limit(50)
)
