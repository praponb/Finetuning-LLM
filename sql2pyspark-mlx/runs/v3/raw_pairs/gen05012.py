from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") <= 1551000)
    .groupBy(
        F.when(F.col("d.budget") >= 3351000, "high").when(F.col("d.budget") >= 1050000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("budget_bucket").desc())
)
