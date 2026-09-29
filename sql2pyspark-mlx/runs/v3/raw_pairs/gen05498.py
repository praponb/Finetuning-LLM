from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1758000)
    .groupBy("location", "dept_name")
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.avg("budget").alias("avg_budget"),
    )
)
