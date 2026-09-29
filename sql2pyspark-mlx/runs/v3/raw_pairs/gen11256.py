from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Sales", "Engineering"]) & (F.col("budget") <= 2827000) & (F.col("budget") < 1733000))
)

agg = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3697000, "high").when(F.col("budget") >= 1634000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
)

result = (
    agg
    .select("budget_bucket", "location", "total_budget")
    .orderBy("budget_bucket", "location")
    .limit(10)
)
