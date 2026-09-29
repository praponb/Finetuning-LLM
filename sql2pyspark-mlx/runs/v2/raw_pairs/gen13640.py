from pyspark.sql import functions as F

result = (
    df_departments
    .cube(
        F.when(F.col("budget") >= 3298000, "high").when(F.col("budget") >= 656000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
)
