from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Marketing") & F.col("location").isNull() & (F.col("dept_name") == "Engineering"))
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
    )
    .orderBy(F.col("finance_budget").desc())
)
