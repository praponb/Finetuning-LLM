from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location", "dept_name").orderBy("rn", "id")
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.partitionBy("location").orderBy("prev_budget", "id")
w5 = Window.partitionBy("location")
w6 = Window.partitionBy("dept_name").orderBy(F.col("row_seq").desc(), "id")
w7 = Window.orderBy("next_budget", "id")
w8 = Window.partitionBy("dept_name")
w9 = Window.partitionBy("location").orderBy(F.col("location_min_prev_budget").desc())
w_hist = Window.partitionBy("dept_name").orderBy("location_min_prev_budget", "id")
w_part = Window.orderBy("row_seq_bucket", "id").rowsBetween(-2, Window.currentRow)
w_part_2 = Window.partitionBy("dept_name").orderBy("next_prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("location_min_prev_budget_percent_rank", "id")

ranked = (
    df_departments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "dept_name",
        "location",
        F.lag("budget").over(w2).alias("prev_budget"),
        F.lead("budget").over(w2).alias("next_budget"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_windows = (
    metrics
    .filter(F.col("dept_name").isin(["Engineering", "Sales"]))
    .select(
        "*",
        F.stddev_samp("next_budget").over(w3).alias("dept_name_stddev_samp_next_budget"),
        F.row_number().over(w4).alias("event_seq"),
        F.min("prev_budget").over(w5).alias("location_min_prev_budget"),
        F.ntile(10).over(w6).alias("row_seq_bucket"),
        F.lead("prev_budget", 2).over(w7).alias("next_prev_budget"),
    )
)

with_windows_departments = (
    with_windows
    .select(
        "id",
        "dept_name",
        "prev_budget",
        "row_seq",
        "dept_name_stddev_samp_next_budget",
        "location_min_prev_budget",
        "row_seq_bucket",
        "next_prev_budget",
        F.stddev_samp("row_seq").over(w8).alias("dept_name_stddev_samp_row_seq"),
        F.percent_rank().over(w9).alias("location_min_prev_budget_percent_rank"),
    )
)

with_windows_departments_2 = (
    with_windows_departments
    .select(
        "*",
        F.lead("dept_name_stddev_samp_next_budget", 1).over(w_hist).alias("next_dept_name_stddev_samp_next_budget"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w8)), 4).alias("row_seq_share"),
        F.avg("location_min_prev_budget_percent_rank").over(w_part).alias("location_min_prev_budget_percent_rank_avg_2"),
        F.first("dept_name_stddev_samp_row_seq", ignorenulls=True).over(w_part_2).alias("first_known_dept_name_stddev_samp_row_seq"),
        F.ntile(5).over(w13).alias("location_min_prev_budget_percent_rank_bucket"),
    )
)

result = (
    with_windows_departments_2
    .select(
        "id",
        "dept_name",
        "dept_name_stddev_samp_next_budget",
        "location_min_prev_budget",
        "row_seq_bucket",
        "dept_name_stddev_samp_row_seq",
        "location_min_prev_budget_percent_rank",
        F.when(F.col("dept_name").rlike("^(HR|Marketing)$"), "EXTENDED").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "GROUP_A").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy("location_min_prev_budget")
)
