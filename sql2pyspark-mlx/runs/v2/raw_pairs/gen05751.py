from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo"]))
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") >= 16552000)
    .select("dept_name", "london_budget", "total_budget")
    .orderBy("total_budget")
)
