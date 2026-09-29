from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3481000, "high").when(F.col("budget") >= 1837000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        "dept_name",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
