from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Finance", "Marketing"]) & ((F.col("budget") <= 2051000) | (F.col("budget") >= 2153000)))
    .groupBy("location")
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .orderBy("location")
)
