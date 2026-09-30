from pyspark.sql import functions as F

result = (
    df_departments
    .cube("location", "dept_name")
    .agg(F.min("budget").alias("min_budget"))
)
