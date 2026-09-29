from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name").isin(["HR", "Marketing"]))

agg = (
    valid_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = agg.orderBy("location_values", "location")
