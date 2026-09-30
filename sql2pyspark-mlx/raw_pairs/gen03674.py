from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy(F.col("running_salary").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("next_salary").desc_nulls_last(), "id")
w_grp = Window.orderBy(F.col("running_salary").desc(), "id")
w6 = Window.orderBy(F.col("running_salary").desc_nulls_first())
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_salary_percent_rank", "id").rowsBetween(-5, -1)

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
        F.initcap(F.trim("job_title")).alias("job_title_title"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "job_title_title",
        F.lead("salary").over(w).alias("next_salary"),
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
)

base = (
    with_windows
    .filter(F.col("hire_date") >= F.lit("2023-05-16").cast("date"))
    .select("*", F.upper("email").alias("email_upper"))
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "full_name",
        "is_remote",
        "prev_salary",
        "event_seq",
        "running_salary",
        "email_upper",
        "rn",
    )
)

with_windows_employees = (
    ranked
    .select(
        "id",
        "full_name",
        "prev_salary",
        "event_seq",
        "running_salary",
        "email_upper",
        F.ntile(5).over(w_grp).alias("running_salary_bucket"),
        F.percent_rank().over(w6).alias("running_salary_percent_rank"),
        F.last("running_salary", ignorenulls=True).over(w7).alias("last_known_running_salary"),
    )
)

result = (
    with_windows_employees
    .select(
        "id",
        "prev_salary",
        "event_seq",
        "running_salary",
        "email_upper",
        "running_salary_bucket",
        "last_known_running_salary",
        F.max("event_seq").over(w8).alias("running_max_event_seq"),
        F.count("event_seq").over(w8).alias("running_count_event_seq"),
        F.avg("event_seq").over(w8).alias("running_avg_event_seq"),
        F.sum("event_seq").over(w8).alias("running_sum_event_seq"),
        F.avg("event_seq").over(w9).alias("event_seq_avg_5_prior"),
    )
)
