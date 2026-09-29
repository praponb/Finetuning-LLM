from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy(F.col("budget_bucket").desc_nulls_last(), "id")
w5 = Window.orderBy("employee_hire_date", "id")

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

with_windows = (
    latest_match
    .withColumn("prev_budget", F.lag("budget").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("budget_bucket", F.ntile(10).over(w3))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "employee_hire_date", "budget_bucket")
)

scored = (
    ranked
    .select(
        "id",
        "employee_hire_date",
        "budget_bucket",
        F.lag("budget_bucket").over(w5).alias("prev_budget_bucket"),
        F.lead("budget_bucket").over(w5).alias("next_budget_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget_bucket",
        "s.prev_budget_bucket",
        "s.event_seq",
        F.col("c.hire_date"),
    )
)

grouped = (
    with_latest
    .rollup(
        F.when(F.col("prev_budget_bucket") >= 8, "HIGH").otherwise("LOW").alias("prev_budget_bucket_tier"),
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.when(F.col("budget_bucket") >= 8, "HIGH").otherwise("LOW").alias("budget_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("budget_bucket"), 2).alias("avg_budget_bucket"),
        F.percentile_approx("budget_bucket", 0.95).alias("budget_bucket_p95"),
    )
)

result = grouped.select("budget_bucket_tier", "distinct_ids", "distinct_ids_2")
