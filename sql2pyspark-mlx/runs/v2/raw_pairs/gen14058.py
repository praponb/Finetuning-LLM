from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "London") & F.col("budget").between(2791000, 3097000))
    .select("location", "budget", "dept_name")
)
