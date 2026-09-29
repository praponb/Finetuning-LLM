from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("budget_count")
)
