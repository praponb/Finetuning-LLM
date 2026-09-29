from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("years_since_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("week", "hire_date"), "job_title").orderBy(F.col("hire_date").desc(), "id")
w_part = Window.partitionBy("level").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("dept_id").isin([13, 22, 33]))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

cleaned = (
    filtered
    .filter(F.col("level").isNotNull() & (F.length("full_name") >= 9))
    .withColumn("years_since_hire_date", (F.datediff(F.current_date(), "hire_date") / 365.25))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("OTHER")))
)

enriched = (
    cleaned
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        "level",
        "is_remote_label",
        "resolved_job_title",
        F.first("years_since_hire_date", ignorenulls=True).over(w).alias("first_known_years_since_hire_date"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_employees = (
    enriched
    .filter(F.col("job_title") == "Manager")
    .select(
        "id",
        "level",
        "is_remote_label",
        "first_known_years_since_hire_date",
        F.min("event_seq").over(w_part).alias("running_min_event_seq"),
        F.max("event_seq").over(w_part).alias("running_max_event_seq"),
        F.count("event_seq").over(w_part).alias("running_count_event_seq"),
    )
)

result = (
    enriched_employees
    .groupBy("level")
    .agg(
        F.sum("running_count_event_seq").alias("total_running_count_event_seq"),
        F.percentile_approx("running_count_event_seq", [0.25, 0.5, 0.75]).alias("running_count_event_seq_quartiles"),
        F.round(F.sum("running_count_event_seq") / F.nullif(F.count(F.when(F.col("level") == "L4", 1)), F.lit(0)), 2).alias("running_count_event_seq_per_l4"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
