from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3138000, 3669000))
    .rollup("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
    .orderBy(F.col("dept_name").desc())
)
