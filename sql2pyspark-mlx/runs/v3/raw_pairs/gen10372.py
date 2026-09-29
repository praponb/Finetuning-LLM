from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2664000, 3251000))
    .select("id", "location", "dept_name", "budget")
)
