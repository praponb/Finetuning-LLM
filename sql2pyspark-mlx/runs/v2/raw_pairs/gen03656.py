from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy("chiang_mai_count")
)
