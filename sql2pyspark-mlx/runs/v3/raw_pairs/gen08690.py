from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name") == "Finance")

agg = (
    recent_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3104000, "high").when(F.col("budget") >= 1433000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    agg
    .orderBy(F.col("max_budget").desc(), "location", "budget_bucket")
    .limit(10)
)
