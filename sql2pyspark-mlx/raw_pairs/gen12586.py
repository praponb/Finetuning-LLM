from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.min("budget").alias("min_budget"))
    .filter(F.col("min_budget") >= 3011000)
)
