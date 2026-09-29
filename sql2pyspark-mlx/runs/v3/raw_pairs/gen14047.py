from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1734000, 2281000))
    .select("id", "location", "budget", "dept_name")
)
