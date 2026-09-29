from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
)
