from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)
