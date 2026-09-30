from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Sales")
    .groupBy("location")
    .agg(
        F.count("budget").alias("budget_count"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("budget_count") > 4)
    .select("location", "budget_count", "max_budget")
)
