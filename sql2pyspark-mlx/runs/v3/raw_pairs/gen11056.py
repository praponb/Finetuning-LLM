from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Sales", "Marketing"]))
    .groupBy("location")
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") >= 2)
)
