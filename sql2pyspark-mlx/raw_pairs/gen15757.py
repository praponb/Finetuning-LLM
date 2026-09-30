from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & ((F.col("budget") > 2557000) | F.col("dept_name").isin(["HR", "Sales"])))
)

departments_stats = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3040000, "high").when(F.col("budget") >= 1572000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.min("budget").alias("min_budget"))
)

result = (
    departments_stats
    .filter(F.col("min_budget") > 1144000)
    .select("budget_bucket", "min_budget")
    .orderBy(F.col("budget_bucket").desc())
)
