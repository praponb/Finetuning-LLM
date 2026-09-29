from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("employee_hire_date").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("hire_date", "id")
w5 = Window.partitionBy("job_title").orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq")
w9 = Window.orderBy("running_count_event_seq", "id")
w10 = Window.orderBy("running_avg_event_seq", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-03").cast("date")) & F.col("manager_id").isNotNull())
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("NONE")))
)

enriched = (
    valid
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.*",
        F.col("b.level").alias("employee_level"),
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "employee_full_name",
        "employee_hire_date",
    )
)

with_windows = (
    latest
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "is_remote",
        "employee_full_name",
        F.row_number().over(w2).alias("row_seq"),
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
        F.row_number().over(w_part).alias("event_seq"),
        F.last("job_title", ignorenulls=True).over(w5).alias("last_known_job_title"),
        F.first("job_title", ignorenulls=True).over(w_recent).alias("first_known_job_title_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "row_seq",
        "first_known_job_title_2",
        F.max("event_seq").over(w_hist).alias("running_max_event_seq"),
        F.count("event_seq").over(w_hist).alias("running_count_event_seq"),
        F.avg("event_seq").over(w_hist).alias("running_avg_event_seq"),
        F.sum("event_seq").over(w_hist).alias("running_sum_event_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("row_seq") == 6)
    .select(
        "id",
        "row_seq",
        "running_max_event_seq",
        "running_count_event_seq",
        "running_avg_event_seq",
        "running_sum_event_seq",
        F.floor("running_avg_event_seq").alias("running_avg_event_seq_whole"),
    )
)

with_windows_employees = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w8).alias("row_seq_dense_rank"),
        F.row_number().over(w9).alias("seq_no"),
        F.lead("running_count_event_seq", 1, 0).over(w10).alias("next_running_count_event_seq"),
    )
)

result = (
    with_windows_employees
    .select(
        "id",
        "running_avg_event_seq",
        "running_max_event_seq",
        "running_sum_event_seq",
    )
    .unpivot(
        ids=["id"],
        values=["running_avg_event_seq", "running_max_event_seq", "running_sum_event_seq"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
