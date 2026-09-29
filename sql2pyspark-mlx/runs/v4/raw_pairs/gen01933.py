from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Sales"]))
    .groupBy(
        F.when(F.col("budget") >= 3992000, "high").when(F.col("budget") >= 1893000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
    )
    .orderBy("budget_bucket")
)
