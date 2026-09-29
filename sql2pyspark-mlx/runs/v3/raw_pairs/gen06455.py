from pyspark.sql import functions as F

result = (
    df_departments
    .rollup(
        "dept_name",
        F.when(F.col("budget") >= 4405000, "high").when(F.col("budget") >= 873000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
