from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("budget") >= 1849000) & (F.col("budget") >= 2231000) & ~F.col("location").isin(["Tokyo", "Singapore", "New York"]))
)

departments_stats = (
    recent_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3770000, "high").when(F.col("budget") >= 1167000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)

result = (
    departments_stats
    .filter(F.col("singapore_count") >= 24)
    .orderBy(F.col("singapore_count").desc())
)
