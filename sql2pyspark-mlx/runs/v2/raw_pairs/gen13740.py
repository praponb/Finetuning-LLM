from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") >= 12939000)
)
