from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Marketing"))))
    .select("*", F.lower("dept_name").alias("dept_name_lower"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("budget") >= 3987000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_avg_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("tokyo_avg_budget") < 1000000)
)
