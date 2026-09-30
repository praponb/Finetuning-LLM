from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("budget") < 3212000)

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3452000, "high").when(F.col("budget") >= 874000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
)

result = budget_bucket_summary.orderBy(F.col("unique_locations").desc())
