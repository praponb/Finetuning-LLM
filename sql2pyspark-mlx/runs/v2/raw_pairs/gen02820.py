from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Finance")
    .agg(
        F.count("*").alias("department_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .orderBy("department_count")
)
