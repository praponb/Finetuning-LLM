from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
    .groupBy("dept_name", "location")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
