from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Sales"]))
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("london_budget") < 54)
    .select("location", "total_budget", "london_budget")
)
