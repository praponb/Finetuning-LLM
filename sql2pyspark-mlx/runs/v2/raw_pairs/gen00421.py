from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "HR", "Sales"]) & (F.col("dept_name").isin(["Engineering", "HR"]) | (F.col("budget") > 3734000)))
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .orderBy(F.col("stddev_budget").desc())
)
