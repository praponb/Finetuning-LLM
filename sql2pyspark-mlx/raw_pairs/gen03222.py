from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
