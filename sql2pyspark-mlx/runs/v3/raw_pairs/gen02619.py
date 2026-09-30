from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy("location")
    .limit(10)
)
