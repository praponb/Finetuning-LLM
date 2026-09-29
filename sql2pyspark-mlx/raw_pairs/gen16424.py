from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .groupBy(
        F.when(F.col("d.budget") >= 3510000, "high").when(F.col("d.budget") >= 716000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.dept_name"),
    )
    .agg(F.countDistinct(F.col("e.salary")).alias("unique_managers"))
)
