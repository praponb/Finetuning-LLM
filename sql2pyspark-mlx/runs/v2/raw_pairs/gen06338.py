from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Bangkok", "London"]))
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("avg_budget") > 71)
    .orderBy(F.col("avg_budget").desc())
)
