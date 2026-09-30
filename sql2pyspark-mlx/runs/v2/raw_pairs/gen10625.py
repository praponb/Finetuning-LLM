from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1279000) & F.col("dept_name").isNotNull() & (F.col("budget") > 3371000))
    .select("id", "dept_name", "budget", F.trim("location").alias("clean_location"))
    .orderBy(F.col("id").desc(), "budget")
)
