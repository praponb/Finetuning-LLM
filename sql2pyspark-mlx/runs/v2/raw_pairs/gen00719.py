from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2610000) & (F.col("budget") < 3243000))
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy("location", F.col("berlin_budget").desc())
)
