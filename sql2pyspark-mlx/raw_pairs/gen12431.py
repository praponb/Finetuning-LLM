from pyspark.sql import functions as F

filtered_departments = df_departments.filter((F.col("budget") > 2633000) | (F.col("dept_name") == "Legal"))

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3622000, "high").when(F.col("budget") >= 1606000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = (
    summary
    .filter(F.col("max_budget") < 1745000)
    .select("budget_bucket", "dept_name", "max_budget", "dept_name_values")
    .orderBy("budget_bucket", "dept_name")
    .limit(5)
)
