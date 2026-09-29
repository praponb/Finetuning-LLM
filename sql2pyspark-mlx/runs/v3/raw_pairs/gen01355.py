from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 3500000, "high").when(F.col("budget") >= 1087000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Marketing", "Finance"])
    .agg(F.max("budget"))
)
