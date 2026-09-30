from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count_if(F.col("dept_name") == "Finance").alias("finance_events"),
        F.max("budget").alias("max_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.round(F.avg("budget"), 2).alias("avg_budget_2"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
    .filter(F.col("legal_count") >= 1)
)
