from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("manager_id", "job_title").orderBy("is_remote_flag", "id")
w2 = Window.partitionBy("manager_id").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("job_title", "employee_job_title").orderBy(F.col("next_is_remote_flag").desc(), "id")
w4 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("job_title").orderBy("salary", "id")
w_hist = Window.partitionBy("employee_job_title").orderBy("next_is_remote_flag", "id")

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-05-31").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.lower("level").alias("level_lower"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

filtered = (
    base
    .filter(~F.col("is_remote"))
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

metrics = (
    filtered
    .withColumn("next_is_remote_flag", F.lead("is_remote_flag").over(w_ordered))
    .withColumn("prev_is_remote_flag", F.lag("is_remote_flag").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("running_is_remote_flag", F.sum("is_remote_flag").over(w2))
)

joined = (
    metrics
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.email").like("%@outlook.com"))
    .select("a.*", F.col("e.job_title").alias("employee_job_title"), F.col("e.salary"))
)

with_windows = (
    joined
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("first_known_next_is_remote_flag", F.first("next_is_remote_flag", ignorenulls=True).over(w4))
    .withColumn("next_running_is_remote_flag", F.lead("running_is_remote_flag", 1).over(w_grp))
    .withColumn("prev_event_seq", F.lag("event_seq", 1).over(w_hist))
)

result = (
    with_windows
    .withColumn("first_known_next_is_remote_flag_tier", F.when(F.col("first_known_next_is_remote_flag") >= 79, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["employee_job_title", "level", "first_known_next_is_remote_flag_tier"], ["employee_job_title", "level"], ["employee_job_title"]],
        "employee_job_title",
        "level",
        "first_known_next_is_remote_flag_tier",
    )
    .agg(
        F.round(F.avg("next_is_remote_flag"), 2).alias("avg_next_is_remote_flag"),
        F.count(F.when(F.col("employee_job_title") == "Designer", 1)).alias("designer_count"),
        F.sum(F.when(F.col("level") == "L3", F.col("event_seq")).otherwise(0)).alias("l3_event_seq"),
        F.max("salary").alias("max_salary"),
        F.percentile_approx("first_known_next_is_remote_flag", 0.5).alias("first_known_next_is_remote_flag_p50"),
        F.sum(F.when(F.col("employee_job_title") == "Designer", F.col("is_remote_flag")).otherwise(0)).alias("designer_is_remote_flag"),
    )
    .orderBy(F.col("designer_is_remote_flag").asc_nulls_first())
)
