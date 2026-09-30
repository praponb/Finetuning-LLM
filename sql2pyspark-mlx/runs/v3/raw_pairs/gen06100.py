from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("budget") > 2011000) & (F.col("dept_name") != "Marketing") & (F.col("dept_name") != "Marketing"))
)

departments_stats = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3835000, "high").when(F.col("budget") >= 827000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = (
    departments_stats
    .filter(F.col("avg_budget") < 2720000)
    .select("budget_bucket", "dept_name", "avg_budget", "dept_name_values")
    .orderBy("dept_name_values", F.col("avg_budget").desc())
)
