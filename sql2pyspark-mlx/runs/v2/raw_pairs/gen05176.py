from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.budget") >= 3316000, "high").when(F.col("d.budget") >= 1961000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max(F.col("e.salary")).alias("max_salary"))
    .filter(F.col("max_salary") >= 141000)
    .orderBy("dept_name", "budget_bucket")
)
