from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name") == "Sales")

departments_stats = (
    valid_departments
    .groupBy("location")
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
)

result = (
    departments_stats
    .select("location", "max_budget")
    .orderBy(F.col("location").desc())
    .limit(5)
)
