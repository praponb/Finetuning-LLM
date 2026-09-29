from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") >= 49)
    .orderBy("location", F.col("stddev_budget").desc())
    .limit(5)
)
