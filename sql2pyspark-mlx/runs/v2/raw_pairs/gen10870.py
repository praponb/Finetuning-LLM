from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.budget") >= 3719000, "high").when(F.col("d.budget") >= 1817000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
)
