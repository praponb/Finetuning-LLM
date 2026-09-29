from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["HR", "Sales"])
    .agg(F.sum("budget").alias("value_sum"))
    .select("location", F.col("HR").alias("hr"), F.col("Sales").alias("sales"))
)
