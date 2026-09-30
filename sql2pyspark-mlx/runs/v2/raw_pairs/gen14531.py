from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Singapore", "New York"]))
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy(F.col("avg_budget").desc())
)
