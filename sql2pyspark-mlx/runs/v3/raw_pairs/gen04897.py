from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("avg_budget") < 47)
    .select("location", "avg_budget", "location_list")
    .orderBy(F.col("avg_budget").desc())
)
