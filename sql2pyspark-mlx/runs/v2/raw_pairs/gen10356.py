from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") >= 62)
)
