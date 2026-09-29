from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name").isNull())
    .groupBy(
        F.when(F.col("d.budget") >= 4242000, "high").when(F.col("d.budget") >= 1241000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min(F.col("e.salary")).alias("min_salary"))
    .filter(F.col("min_salary") > 134000)
    .orderBy(F.col("min_salary").desc())
)
