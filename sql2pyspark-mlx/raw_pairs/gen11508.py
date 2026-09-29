from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3847000)
    .groupBy("dept_name", "location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.max("budget").alias("max_budget"),
    )
)
