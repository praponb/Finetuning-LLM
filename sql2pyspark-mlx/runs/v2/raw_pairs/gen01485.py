from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") | ~F.col("location").isin(["London", "Tokyo", "Berlin"]))
    .groupBy(
        F.when(F.col("budget") >= 4048000, "high").when(F.col("budget") >= 1029000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
    )
)
