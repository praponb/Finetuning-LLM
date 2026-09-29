from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("min_budget") >= 2170000)
    .orderBy("stddev_budget")
)
