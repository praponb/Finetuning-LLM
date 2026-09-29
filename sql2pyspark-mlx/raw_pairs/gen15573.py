from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance", "Engineering"]))
    .groupBy("location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("avg_budget") >= 3211000)
    .select("location", "avg_budget", "budget_count")
    .orderBy(F.col("avg_budget").desc(), "location")
    .limit(10)
)
