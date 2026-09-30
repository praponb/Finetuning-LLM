from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
    .orderBy(F.col("engineering_count").desc())
)
