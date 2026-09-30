from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & (F.col("location") == "Tokyo"))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3138000, "high").when(F.col("budget") >= 866000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.min("budget").alias("min_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("min_budget") >= 3741000)
)
