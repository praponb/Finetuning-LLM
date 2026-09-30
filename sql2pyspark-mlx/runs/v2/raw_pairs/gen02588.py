from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.budget") >= 3498000, "high").when(F.col("d.budget") >= 698000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("e.level") == "L5", F.col("e.salary")).otherwise(0)).alias("l5_salary"),
    )
    .filter(F.col("l5_salary") >= 59)
)
