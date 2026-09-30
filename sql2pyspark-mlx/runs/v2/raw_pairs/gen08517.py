from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .groupBy("dept_name")
    .agg(F.min("budget").alias("min_budget"))
    .filter(F.col("min_budget") > 3061000)
    .orderBy("dept_name")
)
