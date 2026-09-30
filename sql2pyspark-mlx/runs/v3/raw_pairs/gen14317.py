from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & F.col("budget").between(1218000, 2279000))
    .select("id", "location", "budget", "dept_name")
)
