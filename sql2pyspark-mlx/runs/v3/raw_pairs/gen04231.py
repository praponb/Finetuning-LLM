from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

departments_stats = (
    base
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)

result = departments_stats.select("location", "dept_name", "tokyo_budget", "dept_name_list")
