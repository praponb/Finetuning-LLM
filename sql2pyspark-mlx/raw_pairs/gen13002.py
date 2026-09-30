from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3733000, "high").when(F.col("budget") >= 1303000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("min_budget") < 1378000)
    .drop("min_budget")
    .orderBy("budget_bucket")
    .limit(1)
)
