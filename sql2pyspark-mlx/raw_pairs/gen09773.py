from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("location").orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name").orderBy("budget_bucket", "id")
w6 = Window.orderBy("budget_bucket", "id")
w7 = Window.partitionBy("dept_name").orderBy("budget_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("dept_name").orderBy("prev_budget_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("prev_budget").desc(), "id")
w10 = Window.orderBy("budget", "id")
w11 = Window.orderBy("dept_name_sum_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.avg("budget").over(w).alias("budget_avg_2_prior"),
        F.ntile(3).over(w2).alias("budget_bucket"),
        F.sum("budget").over(w3).alias("dept_name_sum_budget"),
        F.lag("budget", 1).over(w4).alias("prev_budget"),
    )
)

valid = (
    metrics
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_bucket_rounded", F.round("budget_bucket", 0))
)

metrics_departments = (
    valid
    .filter(F.col("budget") > 3889000)
    .select(
        "*",
        F.lead("dept_name_sum_budget").over(w_recent).alias("next_dept_name_sum_budget"),
        F.lead("budget").over(w_recent).alias("next_budget"),
        F.lag("budget").over(w_recent).alias("prev_budget_2"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

not_flagged = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.job_title").isin(["Designer", "Director"])), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "budget",
        "budget_bucket",
        "dept_name_sum_budget",
        "prev_budget",
        "budget_bucket_rounded",
        "next_dept_name_sum_budget",
        "next_budget",
        F.round((F.col("budget_avg_2_prior") / F.sum("budget_avg_2_prior").over(w3)), 4).alias("budget_avg_2_prior_share"),
        F.lag("row_seq", 1).over(w6).alias("prev_row_seq"),
        F.sum("budget_bucket_rounded").over(w7).alias("running_budget_bucket_rounded"),
        F.last("budget", ignorenulls=True).over(w8).alias("last_known_budget"),
    )
)

base = (
    with_windows
    .filter(F.col("dept_name_sum_budget") <= 12292572)
    .withColumn("last_known_budget_level", F.when(F.col("last_known_budget") >= 3470000, "High").when(F.col("last_known_budget") >= 1386000, "Medium").otherwise("Low"))
)

metrics_departments_2 = (
    base
    .withColumn("prev_budget_bucket", F.ntile(10).over(w9))
    .withColumn("budget_bucket_2", F.ntile(10).over(w10))
    .withColumn("last_known_last_known_budget", F.last("last_known_budget", ignorenulls=True).over(w11))
)

result = (
    metrics_departments_2
    .select(
        "id",
        "budget",
        "dept_name_sum_budget",
        "prev_budget",
        "budget_bucket_rounded",
        "next_budget",
        "prev_row_seq",
        "running_budget_bucket_rounded",
        "last_known_budget",
    )
    .orderBy(
        F.col("budget_bucket_rounded").asc_nulls_first(),
        F.col("last_known_budget").desc(),
    )
)
