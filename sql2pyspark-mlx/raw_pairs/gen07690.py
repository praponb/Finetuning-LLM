from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .groupBy(
        F.when(F.col("budget") >= 4462000, "high").when(F.col("budget") >= 700000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.count("*").alias("num_departments"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("london_budget") < 44)
)
