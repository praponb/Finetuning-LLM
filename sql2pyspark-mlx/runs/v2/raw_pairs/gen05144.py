from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal", "HR"]))
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") > 23059000)
)
