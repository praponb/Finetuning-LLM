from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .groupBy("location")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") < 37)
)
