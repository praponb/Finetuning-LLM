from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 3383000, "high").when(F.col("budget") >= 1870000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["New York", "Tokyo", "Bangkok", "Berlin"])
    .agg(F.sum("budget"))
    .orderBy("budget_bucket")
)
