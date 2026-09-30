from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Chiang Mai"]))
    .groupBy("location")
    .agg(F.avg("budget").alias("avg_budget"))
)
