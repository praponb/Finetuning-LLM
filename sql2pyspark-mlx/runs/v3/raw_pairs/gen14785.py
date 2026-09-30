from pyspark.sql import functions as F

result = (
    df_departments
    .rollup("location", "dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy("unique_locations", "location_values")
)
