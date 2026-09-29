from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy(F.col("dept_name_values").desc())
)
