from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4372000, "high").when(F.col("budget") >= 1893000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("budget_bucket", F.col("avg_budget").desc())
)
