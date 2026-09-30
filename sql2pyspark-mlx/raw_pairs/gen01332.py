from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .filter(F.col("d.budget").isNull())
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.budget") >= 4091000, "high").when(F.col("d.budget") >= 1272000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"))
)
