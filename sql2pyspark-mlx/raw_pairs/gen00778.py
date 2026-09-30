from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") > 1248000)
    .orderBy("location")
)
