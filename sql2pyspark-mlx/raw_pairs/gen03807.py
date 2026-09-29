from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("dept_id").orderBy("salary", "id")
w_hist = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "manager_id").orderBy("salary", "id")
w5 = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("seq_no").asc_nulls_last())
w7 = Window.partitionBy("level").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("level").orderBy("seq_no_rank", "id")
w9 = Window.orderBy("seq_no_rank", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-10").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level").isin(["L3", "L1"]), "Group A").otherwise("Other").alias("level_group"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("salary").over(w_hist).alias("running_salary_2"),
        F.lead("salary", 2).over(w4).alias("next_salary"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "email",
        "hire_date",
        "is_remote",
        "level",
        "level_group",
        "running_salary",
        "seq_no",
        "next_salary",
        F.last("next_salary", ignorenulls=True).over(w5).alias("last_known_next_salary"),
        F.rank().over(w6).alias("seq_no_rank"),
    )
)

base = (
    with_windows
    .filter((F.col("hire_date") > F.lit("2023-06-09").cast("date")) & F.col("level").isNotNull())
    .select(
        "*",
        F.date_format("hire_date", "yyyy-MM-dd").alias("hire_date_label"),
        (F.col("running_salary") * F.col("last_known_next_salary")).alias("running_salary_times_last_known_next_salary"),
    )
)

with_windows_employees = (
    base
    .filter(F.col("hire_date") <= "2025-01-01")
    .select(
        "id",
        "email",
        "hire_date",
        "is_remote",
        "level",
        "seq_no",
        "next_salary",
        "seq_no_rank",
        "hire_date_label",
        "running_salary_times_last_known_next_salary",
        F.avg("next_salary").over(w7).alias("next_salary_avg_2"),
        F.row_number().over(w8).alias("event_seq"),
        F.ntile(5).over(w9).alias("seq_no_rank_bucket"),
    )
)

result = (
    with_windows_employees
    .groupBy(
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        "level",
    )
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.sum(F.when(F.col("level") == "L1", F.col("seq_no_rank"))).alias("l1_total_seq_no_rank"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("l1_total_seq_no_rank").asc_nulls_first())
)
