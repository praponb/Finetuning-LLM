from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("e.level") == "L5")
    .groupBy(
        F.when(F.col("d.budget") >= 4431000, "high").when(F.col("d.budget") >= 1316000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.location"),
    )
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
)
