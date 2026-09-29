from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "HR", "Sales"]))
    .select("budget", "dept_name")
)
