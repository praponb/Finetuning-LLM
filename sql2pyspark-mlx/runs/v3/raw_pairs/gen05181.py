from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Engineering"]) & F.col("location").isin(["Bangkok", "Tokyo"]))
    .select("id", "dept_name", F.lower("location").alias("location_lower"))
)
