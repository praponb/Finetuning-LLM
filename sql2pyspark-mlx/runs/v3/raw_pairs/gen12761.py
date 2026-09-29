from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 6)
    .orderBy("stddev_budget", "dept_name")
    .limit(20)
)
