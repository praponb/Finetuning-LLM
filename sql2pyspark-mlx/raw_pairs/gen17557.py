from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & F.col("location").isNotNull())
    .select("id", "location", "dept_name", "budget")
)
