from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Bangkok", "Berlin"]))
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("dept_name")
)
