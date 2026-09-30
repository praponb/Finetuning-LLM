from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Sales")
    .groupBy(
        F.when(F.col("budget") >= 4191000, "high").when(F.col("budget") >= 1317000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .filter(F.col("unique_locations") < 4)
    .orderBy(F.col("budget_bucket").desc())
    .limit(10)
)
