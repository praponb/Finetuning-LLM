from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4397000, "high").when(F.col("budget") >= 954000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        "dept_name",
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
    )
)
