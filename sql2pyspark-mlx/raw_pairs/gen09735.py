from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4386000, "high").when(F.col("budget") >= 1981000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
        F.count("*").alias("cnt"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") < 74)
    .orderBy(F.col("finance_count").desc(), "budget_bucket")
)
