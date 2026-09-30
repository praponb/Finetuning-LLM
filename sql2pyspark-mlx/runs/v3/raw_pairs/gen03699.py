from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1908000, 3438000))
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
    .filter(F.col("engineering_count") < 5)
    .orderBy(F.col("dept_name_values").desc())
)
