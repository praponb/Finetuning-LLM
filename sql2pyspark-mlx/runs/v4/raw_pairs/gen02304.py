from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget")

with_windows = (
    df_departments
    .withColumn("budget_bucket", F.ntile(4).over(w))
    .withColumn("budget_cume_dist", F.cume_dist().over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("budget_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.max(F.when(F.col("location") == "Singapore", F.col("budget_cume_dist"))).alias("singapore_max_budget_cume_dist"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .orderBy(F.col("engineering_events").asc_nulls_first())
)
