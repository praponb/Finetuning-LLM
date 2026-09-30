from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Marketing"]) & (F.col("budget") <= 3175000))
)

summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3523000, "high").when(F.col("budget") >= 879000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count("*").alias("total_count"))
)

result = summary.filter(F.col("total_count") < 7)
