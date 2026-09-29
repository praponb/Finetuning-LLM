from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
    .filter(F.col("tokyo_budget") >= 48)
    .orderBy("dept_name")
)
