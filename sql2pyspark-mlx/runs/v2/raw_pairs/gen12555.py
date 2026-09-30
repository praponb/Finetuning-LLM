from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget").between(1822000, 2229000))

departments_stats = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = (
    departments_stats
    .filter(F.col("budget_range") >= 37)
    .orderBy(F.col("total_budget").desc())
)
