from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")
w_running = Window.partitionBy("dept_name")
w4 = Window.orderBy("budget", "id")
w_hist = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("location", "dept_name").orderBy("budget_bucket", "id")
w_ordered = Window.orderBy(F.col("running_next_budget").desc(), "id")
w8 = Window.partitionBy("location", "dept_name").orderBy(F.col("running_next_budget_bucket").asc_nulls_last())
w9 = Window.partitionBy("dept_name").orderBy(F.col("running_next_budget").desc())
w10 = Window.partitionBy("location").orderBy("dept_name_avg_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_windows = (
    df_departments
    .withColumn("budget_bucket", F.ntile(10).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("dept_name_sum_budget", F.sum("budget").over(w_running))
    .withColumn("next_budget", F.lead("budget", 1).over(w4))
)

metrics = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_bucket",
        "seq_no",
        "dept_name_sum_budget",
        F.avg("seq_no").over(w_running).alias("dept_name_avg_seq_no"),
        F.sum("next_budget").over(w_hist).alias("running_next_budget"),
    )
)

enriched = (
    metrics
    .withColumn("budget_bucket_bucket", F.ntile(4).over(w_recent))
    .withColumn("running_next_budget_bucket", F.ntile(5).over(w_ordered))
)

scored = (
    enriched
    .withColumn("running_next_budget_bucket_dense_rank", F.dense_rank().over(w8))
    .withColumn("running_next_budget_dense_rank", F.dense_rank().over(w9))
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w10))
    .withColumn("running_next_budget_bucket_share", F.round((F.col("running_next_budget_bucket") / F.sum("running_next_budget_bucket").over(w_running)), 4))
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

result = (
    with_latest
    .cube("dept_name", "employee_job_title")
    .agg(
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.sum(F.when(F.col("location") == "London", F.col("running_next_budget_bucket"))).alias("london_total_running_next_budget_bucket"),
        F.count(F.when(F.col("employee_job_title") == "Analyst", 1)).alias("analyst_rows"),
    )
)
