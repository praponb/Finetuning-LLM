from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Sales"]) & (F.col("location") == "London"))
    .groupBy("location", "dept_name")
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") < 1636000)
    .orderBy("avg_budget", "dept_name")
)
