from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("location") != "Singapore")

departments_stats = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sales"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = departments_stats.select("dept_name", "pct_sales", "avg_budget")
