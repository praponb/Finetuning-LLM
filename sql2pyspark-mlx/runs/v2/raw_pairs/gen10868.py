from pyspark.sql import functions as F

src = df_departments.filter(F.col("dept_name") != "Sales")

departments_stats = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3305000, "high").when(F.col("budget") >= 804000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("row_count"),
    )
)

result = departments_stats.select("budget_bucket", "budget_count", "row_count")
