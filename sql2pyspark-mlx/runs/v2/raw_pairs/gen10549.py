from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3359000)
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
    )
)
