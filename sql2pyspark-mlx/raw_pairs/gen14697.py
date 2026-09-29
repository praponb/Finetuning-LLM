from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York", "Chiang Mai"]))
    .groupBy("location")
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") > 4)
)
