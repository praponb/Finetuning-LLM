from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 3444000, "high").when(F.col("d.budget") >= 1809000, "mid").otherwise("low").alias("budget_bucket"),
        F.col("d.location"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
