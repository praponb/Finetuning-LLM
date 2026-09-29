from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Singapore", "Berlin"]) & (F.col("dept_name") != "Finance"))
    .groupBy("location")
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") < 2470000)
    .orderBy("location")
)
