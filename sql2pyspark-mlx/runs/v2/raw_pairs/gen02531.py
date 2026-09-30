from pyspark.sql import functions as F

src = df_departments.filter(F.col("budget").between(1991000, 2593000))

budget_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 4383000, "high").when(F.col("budget") >= 1147000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
)

result = budget_bucket_summary.orderBy("budget_bucket")
