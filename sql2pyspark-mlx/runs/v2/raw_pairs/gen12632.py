from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Berlin", "Bangkok"]))
    .groupBy(F.when(F.col("budget") >= 3503000, "high").when(F.col("budget") >= 773000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["Tokyo", "New York"])
    .agg(F.max("budget"))
    .orderBy("budget_bucket")
)
