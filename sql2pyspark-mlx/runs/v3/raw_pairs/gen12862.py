from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("dept_name") != "Marketing") & F.col("budget").between(1754000, 2381000) & F.col("dept_name").isin(["Marketing", "Sales"]))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3185000, "high").when(F.col("budget") >= 1672000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    summary
    .filter(F.col("stddev_budget") < 49)
    .select("budget_bucket", "location_list")
    .orderBy(F.col("budget_bucket").desc())
)
