from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Singapore"]))
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy("dept_name", F.col("total_budget").desc())
)
