from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3573000)
    .groupBy("location")
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
