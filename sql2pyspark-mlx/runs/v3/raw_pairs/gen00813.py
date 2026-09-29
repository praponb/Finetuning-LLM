from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("dept_name") != "Legal") & ~F.col("dept_name").isin(["Finance", "HR", "Marketing"]))
    .withColumn("budget_rounded", F.round("budget", 2))
)

budget_rounded_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget_rounded") >= 81, "high").when(F.col("budget_rounded") >= 16, "mid").otherwise("low").alias("budget_rounded_bucket"),
        "location",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    budget_rounded_bucket_summary
    .filter(F.col("max_budget") < 2239000)
    .orderBy(F.col("dept_name_values").desc(), "budget_rounded_bucket", "location")
    .limit(10)
)
