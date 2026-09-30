from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 3611000, "high").when(F.col("budget") >= 600000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["Singapore", "London", "New York"])
    .agg(F.max("budget"))
)
