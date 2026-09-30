from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Berlin"]))
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
    )
    .orderBy(F.col("location").desc(), F.col("berlin_budget").desc())
)
