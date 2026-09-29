from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id").orderBy("salary")
w_running = Window.partitionBy("manager_id")
w3 = Window.partitionBy("level").orderBy(F.col("hire_date_plus_54d").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy("hire_date_plus_54d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.month("hire_date_plus_54d"), "dept_id").orderBy("salary_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("week", "hire_date"), "level").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("job_title", "level")
w8 = Window.partitionBy(F.year("hire_date"), "job_title").orderBy("seq_no_share", "id")
w_part_2 = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.month("hire_date"), "level")
w11 = Window.partitionBy("job_title", "level").orderBy(F.col("seq_no_share").asc_nulls_first())
w_hist = Window.partitionBy("job_title").orderBy("seq_no_share_bucket", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-06-05"))) & F.col("level").isNotNull())
    .withColumn("hire_date_plus_54d", F.date_add("hire_date", 54))
)

metrics = (
    cleaned
    .withColumn("salary_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("manager_id_avg_salary", F.avg("salary").over(w_running))
    .withColumn("seq_no", F.row_number().over(w3))
)

with_windows = (
    metrics
    .filter(F.col("seq_no") == 7)
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "seq_no",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w_running)), 4).alias("seq_no_share"),
        F.first("level", ignorenulls=True).over(w4).alias("first_known_level"),
        F.sum("salary_percent_rank").over(w_part).alias("cumulative_salary_percent_rank"),
    )
)

metrics_employees = (
    with_windows
    .select(
        "id",
        "job_title",
        "salary",
        "level",
        "seq_no_share",
        "first_known_level",
        "cumulative_salary_percent_rank",
        F.sum("cumulative_salary_percent_rank").over(w6).alias("cumulative_cumulative_salary_percent_rank"),
        F.avg("seq_no").over(w7).alias("job_title_avg_seq_no"),
        F.ntile(10).over(w8).alias("seq_no_share_bucket"),
        F.last("seq_no_share", ignorenulls=True).over(w_part_2).alias("last_known_seq_no_share"),
        F.sum("cumulative_salary_percent_rank").over(w10).alias("hire_date_month_sum_cumulative_salary_percent_rank"),
    )
)

latest = (
    metrics_employees
    .withColumn("row_num", F.rank().over(w11))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "job_title",
        "cumulative_salary_percent_rank",
        "job_title_avg_seq_no",
        "seq_no_share_bucket",
    )
)

result = (
    latest
    .select(
        "id",
        "job_title",
        "cumulative_salary_percent_rank",
        "job_title_avg_seq_no",
        F.lag("seq_no_share_bucket").over(w_hist).alias("prev_seq_no_share_bucket"),
        F.lag("cumulative_salary_percent_rank").over(w_hist).alias("prev_cumulative_salary_percent_rank"),
        F.lead("cumulative_salary_percent_rank").over(w_hist).alias("next_cumulative_salary_percent_rank"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)
