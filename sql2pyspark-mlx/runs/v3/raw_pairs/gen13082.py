from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Legal"]) & F.col("dept_name").isin(["Finance", "HR", "Engineering"]))
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("min_budget").desc())
)
