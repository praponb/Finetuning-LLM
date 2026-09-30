from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .groupBy(F.when(F.col("budget") >= 4411000, "high").when(F.col("budget") >= 915000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["Chiang Mai", "Bangkok", "Singapore"])
    .agg(F.sum("budget"))
    .orderBy("budget_bucket")
)
