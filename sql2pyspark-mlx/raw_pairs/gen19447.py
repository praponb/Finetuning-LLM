from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("manager_id").orderBy("salary", "id")
w_hist = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("salary", "id")
w_running = Window.orderBy("prev_salary", "id")
w5 = Window.orderBy("next_is_remote_flag", "id").rowsBetween(-3, -1)
w6 = Window.orderBy("next_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_prev_salary_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("prev_salary").desc(), "id")
w_grp = Window.orderBy(F.col("event_seq_2").asc_nulls_last())
w10 = Window.orderBy(F.col("cumulative_is_remote_flag_minus_next_is_remote_flag").desc(), "id")
w11 = Window.orderBy(F.col("prev_next_is_remote_flag_2").desc(), "id")
w12 = Window.orderBy("cumulative_is_remote_flag", "id")

base = (
    df_employees
    .filter(F.col("dept_id").isin([16, 40]))
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.upper("full_name").alias("full_name_upper"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

scored = (
    base
    .filter(F.col("level").isin(["L4", "L1", "L5"]))
    .select(
        "id",
        "salary",
        "is_remote_flag",
        "full_name_upper",
        "level_filled",
        F.lead("is_remote_flag").over(w_recent).alias("next_is_remote_flag"),
        F.lead("salary").over(w_recent).alias("next_salary"),
        F.lag("salary").over(w_recent).alias("prev_salary"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("salary").over(w_hist).alias("cumulative_salary"),
    )
)

with_windows = (
    scored
    .withColumn("next_next_is_remote_flag", F.lead("next_is_remote_flag").over(w3))
    .withColumn("prev_next_is_remote_flag", F.lag("next_is_remote_flag").over(w3))
    .withColumn("next_prev_salary", F.lead("prev_salary").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
)

with_windows_employees = (
    with_windows
    .select(
        "id",
        "salary",
        "is_remote_flag",
        "full_name_upper",
        "next_is_remote_flag",
        "prev_salary",
        "prev_next_is_remote_flag",
        F.lead("row_seq").over(w_running).alias("next_row_seq"),
        F.lag("next_is_remote_flag").over(w_running).alias("prev_next_is_remote_flag_2"),
        F.row_number().over(w_running).alias("event_seq"),
        F.avg("next_prev_salary").over(w5).alias("next_prev_salary_avg_3_prior"),
    )
)

metrics = (
    with_windows_employees
    .select(
        "id",
        "is_remote_flag",
        "next_is_remote_flag",
        "prev_salary",
        "prev_next_is_remote_flag",
        "prev_next_is_remote_flag_2",
        "event_seq",
        F.sum("prev_salary").over(w6).alias("cumulative_prev_salary"),
        F.sum("is_remote_flag").over(w7).alias("cumulative_is_remote_flag"),
        F.row_number().over(w8).alias("event_seq_2"),
    )
)

base_employees = (
    metrics
    .filter(F.col("next_is_remote_flag") <= 70)
    .select(
        "id",
        "is_remote_flag",
        "next_is_remote_flag",
        "prev_salary",
        "prev_next_is_remote_flag",
        "prev_next_is_remote_flag_2",
        "event_seq",
        "cumulative_prev_salary",
        "cumulative_is_remote_flag",
        "event_seq_2",
        F.floor("is_remote_flag").alias("is_remote_flag_whole"),
        (F.col("cumulative_is_remote_flag") - F.col("next_is_remote_flag")).alias("cumulative_is_remote_flag_minus_next_is_remote_flag"),
    )
)

result = (
    base_employees
    .withColumn("event_seq_2_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("cumulative_is_remote_flag_minus_next_is_remote_flag_bucket", F.ntile(4).over(w10))
    .withColumn("prev_next_is_remote_flag_2_bucket", F.ntile(4).over(w11))
    .withColumn("next_event_seq", F.lead("event_seq", 1).over(w12))
    .withColumn("next_event_seq_2", F.lead("event_seq_2", 1).over(w_running))
)
