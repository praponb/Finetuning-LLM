from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("days_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("hire_date")).orderBy("salary")
w3 = Window.orderBy("running_min_salary", "id")
w4 = Window.orderBy(F.col("running_sum_salary").desc(), "id")
w5 = Window.orderBy("seq_no", "id")
w6 = Window.orderBy(F.col("seq_no_2").desc(), "id")
w7 = Window.orderBy("next_running_count_salary", "id")
w8 = Window.orderBy("next_running_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("seq_no_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("prev_running_sum_salary").desc_nulls_last())
w11 = Window.orderBy(F.col("seq_no_3").desc(), "id")
w12 = Window.orderBy("prev_running_sum_salary_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("next_running_count_salary_bucket", "id")
w14 = Window.orderBy("prev_running_sum_salary", "id")

base = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "email",
        "hire_date",
        F.min("salary").over(w).alias("running_min_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
        F.sum("salary").over(w).alias("running_sum_salary"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "hire_date",
        "running_sum_salary",
        F.lead("running_count_salary").over(w3).alias("next_running_count_salary"),
        F.lag("running_count_salary").over(w3).alias("prev_running_count_salary"),
        F.lead("running_sum_salary").over(w3).alias("next_running_sum_salary"),
        F.lag("running_sum_salary").over(w3).alias("prev_running_sum_salary"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

filtered = (
    enriched
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-08-09"))) & F.col("prev_running_sum_salary").between(415000, 688000))
    .select(
        "id",
        "next_running_count_salary",
        "prev_running_sum_salary",
        "seq_no",
        "seq_no_2",
        (F.col("prev_running_sum_salary") * F.col("seq_no_2")).alias("prev_running_sum_salary_times_seq_no_2"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "next_running_count_salary",
        "prev_running_sum_salary",
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("seq_no_3"),
        F.ntile(4).over(w7).alias("next_running_count_salary_bucket"),
        F.sum("seq_no").over(w8).alias("running_seq_no"),
    )
)

scored_employees = (
    scored
    .filter(F.col("seq_no_3").between(4, 7))
    .select(
        "*",
        F.sum("row_seq").over(w_recent).alias("running_row_seq"),
        F.dense_rank().over(w10).alias("prev_running_sum_salary_dense_rank"),
    )
)

with_windows_employees = (
    scored_employees
    .select(
        "id",
        "next_running_count_salary",
        "seq_no_3",
        "next_running_count_salary_bucket",
        "prev_running_sum_salary_dense_rank",
        F.ntile(10).over(w11).alias("seq_no_3_bucket"),
        F.sum("prev_running_sum_salary").over(w12).alias("running_prev_running_sum_salary"),
        F.lag("running_seq_no", 2).over(w13).alias("prev_running_seq_no"),
        F.ntile(5).over(w14).alias("prev_running_sum_salary_bucket"),
    )
)

result = (
    with_windows_employees
    .groupBy("prev_running_seq_no")
    .agg(
        F.percentile_approx("prev_running_sum_salary_bucket", 0.5).alias("prev_running_sum_salary_bucket_p50"),
        F.percentile_approx("next_running_count_salary_bucket", 0.95).alias("next_running_count_salary_bucket_p95"),
        F.round(F.avg("prev_running_sum_salary_bucket"), 2).alias("avg_prev_running_sum_salary_bucket"),
    )
    .orderBy("prev_running_seq_no")
)
