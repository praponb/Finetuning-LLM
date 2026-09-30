from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 4111000, "high").when(F.col("budget") >= 2021000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["Berlin", "Chiang Mai", "London"])
    .agg(F.max("budget"))
    .orderBy("budget_bucket")
)
