from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2077000)
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("num_departments"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .filter(F.col("num_departments") < 7)
    .select("dept_name", "num_departments", "singapore_budget")
)
