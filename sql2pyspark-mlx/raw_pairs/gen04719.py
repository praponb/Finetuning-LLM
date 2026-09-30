from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .groupBy("location", "dept_name")
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") < 40)
    .orderBy(F.col("location").desc(), "dept_name")
    .limit(20)
)
