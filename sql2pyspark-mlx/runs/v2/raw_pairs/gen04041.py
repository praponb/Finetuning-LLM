from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "HR") & F.col("budget").isNull())
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)
