from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.min("budget").alias("min_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy(F.col("dept_name").desc())
)
