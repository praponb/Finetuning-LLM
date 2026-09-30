from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3596000, "high").when(F.col("budget") >= 886000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .filter(F.col("marketing_count") < 63)
    .orderBy(F.col("marketing_count").desc())
)
