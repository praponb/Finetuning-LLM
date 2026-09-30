from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w_grp = Window.orderBy("budget", "id").rowsBetween(-5, Window.currentRow)

metrics = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.dense_rank().over(w_running).alias("budget_dense_rank"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.ntile(10).over(w3).alias("budget_bucket"),
        F.avg("budget").over(w_grp).alias("budget_avg_5"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("budget_share") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("budget_share_tier"),
    )
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_rows"),
        F.round(F.avg("budget_avg_5"), 2).alias("avg_budget_avg_5"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget_dense_rank")).otherwise(0)).alias("new_york_budget_dense_rank"),
        F.max(F.when(F.col("location") == "Berlin", F.col("budget_dense_rank"))).alias("berlin_max_budget_dense_rank"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget_bucket")).otherwise(0)).alias("bangkok_budget_bucket"),
    )
    .filter(F.col("budget_p95") > 0)
    .orderBy(F.col("bangkok_budget_bucket").desc())
)
