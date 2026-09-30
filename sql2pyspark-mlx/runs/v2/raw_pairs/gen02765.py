from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4429000, "high").when(F.col("budget") >= 1420000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("budget").alias("unique_locations"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("unique_locations").desc(), "budget_bucket")
    .limit(20)
)
