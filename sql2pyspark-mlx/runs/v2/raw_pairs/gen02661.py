from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3977000, "high").when(F.col("budget") >= 2052000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("chiang_mai_budget") > 62)
    .orderBy("chiang_mai_budget", "dept_name")
)
