from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3371000, "high").when(F.col("budget") >= 1066000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("avg_budget") >= 74)
)
