from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_departments
    .withColumn("running_min_budget", F.min("budget").over(w))
    .withColumn("running_sum_budget", F.sum("budget").over(w))
    .withColumn("running_max_budget", F.max("budget").over(w))
)

result = (
    scored
    .groupBy(
        "dept_name",
        F.when(F.col("running_min_budget") >= 3832000, "LARGE").otherwise("SMALL").alias("running_min_budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("running_sum_budget"))).alias("engineering_total_running_sum_budget"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
    )
)
