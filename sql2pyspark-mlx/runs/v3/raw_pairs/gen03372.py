from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("budget_count")
)
