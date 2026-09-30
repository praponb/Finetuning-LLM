from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") > 66)
    .orderBy("budget_range")
)
