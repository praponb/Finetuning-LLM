from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3696000) & (F.col("dept_name") == "HR"))
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
)
