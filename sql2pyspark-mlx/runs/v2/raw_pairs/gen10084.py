from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1092000, 3271000) & (F.col("dept_name") != "Legal"))
    .select("id", "budget", "location", "dept_name")
)
