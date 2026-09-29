from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy("dept_name")
)
