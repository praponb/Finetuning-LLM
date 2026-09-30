from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("dept_name") != "HR") & ~F.col("dept_name").isin(["Marketing", "HR", "Finance"]))
)

departments_stats = (
    valid_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = departments_stats.orderBy("dept_name_values", "location")
