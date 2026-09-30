from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(~(F.col("budget").between(2557000, 2706000) & (F.col("location") == "Singapore")))
)

departments_stats = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3653000, "high").when(F.col("budget") >= 826000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    departments_stats
    .orderBy("total_budget", "budget_bucket")
    .limit(1)
)
