from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 4495000, "high").when(F.col("budget") >= 754000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["Tokyo", "Chiang Mai", "Singapore"])
    .agg(F.max("budget"))
)
