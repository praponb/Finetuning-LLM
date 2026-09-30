from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3393000, "high").when(F.col("budget") >= 1579000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)
