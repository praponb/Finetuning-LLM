from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1179000)
    .groupBy(
        F.when(F.col("dept_name").isin(["Legal", "Finance"]), "core").otherwise("other").alias("dept_name_group"),
        "location",
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("stddev_budget") > 30)
    .select("dept_name_group", "location", "location_list", "stddev_budget")
)
