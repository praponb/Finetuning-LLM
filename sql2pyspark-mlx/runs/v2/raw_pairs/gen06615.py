from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(
        F.max("budget").alias("max_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .orderBy(F.col("max_budget").desc())
)
