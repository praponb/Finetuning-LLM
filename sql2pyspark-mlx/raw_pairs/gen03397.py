from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .cube(
        F.when(F.col("budget") >= 4219000, "high").when(F.col("budget") >= 665000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["New York", "London"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("budget_bucket")
)
