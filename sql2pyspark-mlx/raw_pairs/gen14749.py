from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2187000, 3721000) & F.col("dept_name").isin(["Finance", "Sales"]))
    .select("budget", "location", "dept_name")
)
