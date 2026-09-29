from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .groupBy("dept_name")
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy("avg_budget")
)
