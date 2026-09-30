from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("location").isin(["London", "Tokyo"]) | F.col("dept_name").isNull())
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 4))
)

departments_stats = (
    valid_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3984000, "high").when(F.col("budget") >= 1865000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("min_budget") < 1976000)
    .select("dept_name", "budget_bucket", "total_count", "min_budget")
    .orderBy(F.col("total_count").desc())
)
