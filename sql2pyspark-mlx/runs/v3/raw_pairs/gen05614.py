from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 3759000, "high").when(F.col("d.budget") >= 851000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc(), "budget_bucket")
    .limit(50)
)
