from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & (F.col("budget").isNotNull() | (F.col("budget") <= 3695000)))
    .select("id", "location", F.lower("dept_name").alias("dept_name_lower"))
)
