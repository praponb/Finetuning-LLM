from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2030000)
    .groupBy(
        F.when(F.col("budget") >= 4218000, "high").when(F.col("budget") >= 1626000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)
