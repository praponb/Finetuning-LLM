from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3276000)
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("dept_name")
)
