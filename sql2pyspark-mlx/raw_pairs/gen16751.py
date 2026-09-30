from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("location", "dept_name").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w5 = Window.partitionBy("dept_name").orderBy("last4_min_event_seq", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("prev_budget").desc(), "id")
w8 = Window.orderBy("last4_max_next_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_min_prev_budget", "id")

metrics = (
    df_departments
    .withColumn("next_budget", F.lead("budget").over(w_running))
    .withColumn("prev_budget", F.lag("budget").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

metrics_departments = (
    metrics
    .filter(F.col("event_seq") <= 6)
    .withColumn("last4_min_event_seq", F.min("event_seq").over(w_recent))
    .withColumn("last4_sum_event_seq", F.sum("event_seq").over(w_recent))
    .withColumn("last4_max_event_seq", F.max("event_seq").over(w_recent))
)

with_latest = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.hire_date"))
)

deduped = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics_departments_2 = (
    deduped
    .select(
        "id",
        "budget",
        "prev_budget",
        "event_seq",
        "last4_max_event_seq",
        F.count("next_budget").over(w5).alias("last4_count_next_budget"),
        F.avg("next_budget").over(w5).alias("last4_avg_next_budget"),
        F.sum("next_budget").over(w5).alias("last4_sum_next_budget"),
        F.max("next_budget").over(w5).alias("last4_max_next_budget"),
    )
)

with_windows = (
    metrics_departments_2
    .withColumn("running_min_prev_budget", F.min("prev_budget").over(w6))
    .withColumn("running_count_prev_budget", F.count("prev_budget").over(w6))
    .withColumn("running_avg_prev_budget", F.avg("prev_budget").over(w6))
    .withColumn("running_max_prev_budget", F.max("prev_budget").over(w6))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

metrics_departments_3 = (
    with_windows
    .select(
        "*",
        F.count("row_seq").over(w8).alias("running_count_row_seq"),
        F.max("row_seq").over(w8).alias("running_max_row_seq"),
        F.min("row_seq").over(w8).alias("running_min_row_seq"),
        F.sum("row_seq").over(w8).alias("running_sum_row_seq"),
        F.lag("last4_avg_next_budget", 2, 0).over(w9).alias("prev_last4_avg_next_budget"),
    )
)

result = (
    metrics_departments_3
    .groupBy(
        F.when(F.col("running_sum_row_seq") >= 33, "PREMIUM").otherwise("STANDARD").alias("running_sum_row_seq_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("prev_budget"), 2).alias("avg_prev_budget"),
        F.percentile_approx("running_max_row_seq", 0.95).alias("running_max_row_seq_p95"),
    )
    .orderBy(F.col("running_sum_row_seq_tier").desc())
)
