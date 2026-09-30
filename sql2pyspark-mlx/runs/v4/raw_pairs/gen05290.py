from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3132000, "high").when(F.col("budget") >= 1594000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("max_budget") < 1625000)
    .select("budget_bucket", "max_budget", "dept_name_list")
    .orderBy("budget_bucket")
)
