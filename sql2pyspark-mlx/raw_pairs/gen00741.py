from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2746000)
    .agg(F.count("budget").alias("budget_count"))
    .orderBy("budget_count")
)
