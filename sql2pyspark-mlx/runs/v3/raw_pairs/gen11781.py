from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3915000, "high").when(F.col("budget") >= 1689000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.sum("budget").alias("total_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("total_budget") >= 21193000)
)
