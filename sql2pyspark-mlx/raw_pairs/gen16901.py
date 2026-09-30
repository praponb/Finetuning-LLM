from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3838000)
    .groupBy("location", "dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("stddev_budget") >= 68)
    .orderBy("location", "dept_name")
    .limit(5)
)
