from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy("budget_count")
)
