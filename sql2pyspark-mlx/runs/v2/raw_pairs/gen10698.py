from pyspark.sql import functions as F

base = df_departments.filter(F.col("location") == "London")

summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3330000, "high").when(F.col("budget") >= 1690000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = summary.orderBy("dept_name", "budget_bucket")
