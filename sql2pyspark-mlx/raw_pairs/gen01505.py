from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3531000, "high").when(F.col("budget") >= 979000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
