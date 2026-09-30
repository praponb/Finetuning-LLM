from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.count("budget").alias("budget_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("budget_count").desc())
)
