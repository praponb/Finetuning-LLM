from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("location").isin(["London", "Chiang Mai", "Berlin"]) & (F.col("dept_name") == "HR")) | F.col("location").isNull())
)

departments_stats = (
    valid_departments
    .groupBy("dept_name", "location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("budget_range") > 47)
    .orderBy("dept_name")
)
