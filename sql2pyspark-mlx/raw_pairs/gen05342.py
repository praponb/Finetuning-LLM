from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("dept_name", "location")
w_recent = Window.partitionBy("dept_name").orderBy("budget", "id")
w6 = Window.partitionBy("location").orderBy("budget_share", "id")
w7 = Window.partitionBy("location")
w8 = Window.partitionBy("location").orderBy("prev_budget_bucket", "id")
w9 = Window.partitionBy("location").orderBy("budget_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("location").orderBy(F.col("event_seq_share").desc_nulls_last())

scored = (
    df_departments
    .filter(F.col("budget") < 2012000)
    .select(
        "id",
        "location",
        F.ntile(5).over(w_ordered).alias("budget_bucket"),
        F.sum("budget").over(w2).alias("location_sum_budget"),
        F.avg("budget").over(w3).alias("budget_avg_2"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
        F.ntile(3).over(w_recent).alias("budget_bucket_2"),
    )
)

with_windows = (
    scored
    .withColumn("next_budget_share", F.lead("budget_share").over(w6))
    .withColumn("next_budget_bucket", F.lead("budget_bucket").over(w6))
    .withColumn("prev_budget_bucket", F.lag("budget_bucket").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
)

with_windows_departments = (
    with_windows
    .select(
        "*",
        F.round((F.col("event_seq") / F.sum("event_seq").over(w7)), 4).alias("event_seq_share"),
        F.lag("budget_share", 1, 0).over(w8).alias("prev_budget_share"),
    )
)

not_flagged = (
    with_windows_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 8), "left_anti")
)

with_windows_departments_2 = (
    not_flagged
    .select(
        "id",
        "location",
        "budget_bucket",
        "location_sum_budget",
        "budget_avg_2",
        "budget_share",
        "budget_bucket_2",
        "next_budget_share",
        "next_budget_bucket",
        "prev_budget_bucket",
        "prev_budget_share",
        F.avg("location_sum_budget").over(w9).alias("running_avg_location_sum_budget"),
        F.count("location_sum_budget").over(w9).alias("running_count_location_sum_budget"),
        F.sum("location_sum_budget").over(w9).alias("running_sum_location_sum_budget"),
        F.rank().over(w10).alias("event_seq_share_rank"),
    )
)

result = (
    with_windows_departments_2
    .groupBy(
        "location",
        F.when(F.col("running_count_location_sum_budget") >= 15, "LARGE").otherwise("SMALL").alias("running_count_location_sum_budget_tier"),
    )
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.round(F.sum("budget_bucket_2") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("budget_bucket_2_per_tokyo"),
        F.avg(F.when(F.col("location") == "London", F.col("prev_budget_bucket"))).alias("london_avg_prev_budget_bucket"),
    )
    .orderBy("location")
)
