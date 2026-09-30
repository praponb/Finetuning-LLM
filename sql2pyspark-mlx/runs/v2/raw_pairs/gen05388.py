from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .orderBy("dept_name_values")
)
