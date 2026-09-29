from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & (F.col("budget") <= 2084000))
    .select("id", "dept_name", "budget", "location")
    .orderBy(F.col("id").desc())
)
