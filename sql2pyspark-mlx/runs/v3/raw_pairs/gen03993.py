from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("budget") > 3111000)
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

budget_filled_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget_filled") >= 71, "high").when(F.col("budget_filled") >= 33, "mid").otherwise("low").alias("budget_filled_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = budget_filled_bucket_summary.orderBy("dept_name_values")
