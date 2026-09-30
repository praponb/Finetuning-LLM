from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("avg_budget")
)
