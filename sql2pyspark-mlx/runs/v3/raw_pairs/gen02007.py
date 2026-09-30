from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy("total_budget")
)
