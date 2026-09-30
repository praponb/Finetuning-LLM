from pyspark.sql import functions as F

result = (
    df_departments
    .cube("dept_name", "location")
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy(F.col("location").desc(), "avg_budget")
)
