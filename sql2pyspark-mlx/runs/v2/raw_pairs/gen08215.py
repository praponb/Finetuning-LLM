from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1972000)
    .groupBy(
        F.when(F.col("budget") >= 4458000, "high").when(F.col("budget") >= 1002000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("total_count") >= 4)
)
