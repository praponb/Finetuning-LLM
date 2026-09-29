from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name").isin(["Engineering", "Finance"]))

departments_stats = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.count("*").alias("cnt"),
    )
)

result = (
    departments_stats
    .filter(F.col("cnt") < 6)
    .orderBy("dept_name_values")
)
