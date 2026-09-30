from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") >= 1789000) & (F.col("dept_name") == "Finance")) | (F.col("budget") > 3165000))
    .groupBy("dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
)
