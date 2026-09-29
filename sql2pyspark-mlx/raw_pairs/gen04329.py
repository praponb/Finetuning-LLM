from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget", "id")
w_running = Window.orderBy("running_sum_budget", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("running_max_budget").desc(), "id")
w6 = Window.orderBy(F.col("running_sum_budget_avg_6").desc())

enriched = (
    df_departments
    .select(
        "id",
        "budget",
        F.max("budget").over(w_recent).alias("running_max_budget"),
        F.count("budget").over(w_recent).alias("running_count_budget"),
        F.sum("budget").over(w_recent).alias("running_sum_budget"),
        F.ntile(10).over(w2).alias("budget_bucket"),
    )
)

valid = (
    enriched
    .filter(F.col("budget") > 2654000)
    .withColumn("greatest_budget_bucket_running_max_budget", F.greatest("budget_bucket", "running_max_budget"))
)

with_windows = (
    valid
    .filter(F.col("budget_bucket").isNotNull())
    .withColumn("running_sum_budget_avg_6", F.avg("running_sum_budget").over(w_running))
    .withColumn("cumulative_running_max_budget", F.sum("running_max_budget").over(w4))
)

enriched_departments = (
    with_windows
    .select(
        "id",
        "running_count_budget",
        "running_sum_budget",
        "running_sum_budget_avg_6",
        F.sum("cumulative_running_max_budget").over(w4).alias("cumulative_cumulative_running_max_budget"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.dense_rank().over(w6).alias("running_sum_budget_avg_6_dense_rank"),
    )
)

kept = (
    enriched_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([17, 38])), "left_anti")
)

base = (
    kept
    .filter(F.col("running_sum_budget_avg_6") >= 7742864)
    .select(
        "id",
        "running_count_budget",
        "running_sum_budget",
        "running_sum_budget_avg_6",
        "seq_no",
        "running_sum_budget_avg_6_dense_rank",
        (F.col("cumulative_cumulative_running_max_budget") + F.col("running_sum_budget_avg_6_dense_rank")).alias("cumulative_cumulative_running_max_budget_plus_running_sum_budget_avg_6_dense_rank"),
        F.round((F.col("cumulative_cumulative_running_max_budget") * 100), 2).alias("adjusted_cumulative_cumulative_running_max_budget"),
    )
)

summary = (
    base
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("running_sum_budget", 0.95).alias("running_sum_budget_p95"),
        F.avg("running_sum_budget_avg_6").alias("avg_running_sum_budget_avg_6"),
        F.round(F.avg("adjusted_cumulative_cumulative_running_max_budget"), 2).alias("avg_adjusted_cumulative_cumulative_running_max_budget"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)

result = (
    summary
    .select(
        "seq_no",
        "running_sum_budget_p95",
        "avg_adjusted_cumulative_cumulative_running_max_budget",
        "distinct_ids",
        "distinct_ids_2",
    )
    .orderBy(
        F.col("distinct_ids").desc_nulls_last(),
        F.col("avg_adjusted_cumulative_cumulative_running_max_budget").desc(),
    )
)
