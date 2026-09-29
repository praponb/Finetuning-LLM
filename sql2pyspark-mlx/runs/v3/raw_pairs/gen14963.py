from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York"]))
    .groupBy("location", "dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("marketing_count").desc())
)
