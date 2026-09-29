from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("tokyo_count") >= 9)
    .orderBy(F.col("tokyo_count").desc(), "avg_budget", "dept_name")
    .limit(20)
)
