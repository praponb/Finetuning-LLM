from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
    .filter(F.col("engineering_count") >= 22)
    .drop("engineering_count")
)
