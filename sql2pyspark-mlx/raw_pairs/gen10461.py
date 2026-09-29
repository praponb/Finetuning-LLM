from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy(F.col("is_remote_flag").desc())

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-04-29").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.concat("job_title", F.lit(" "), "level").alias("job_title_level"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

valid = (
    filtered
    .filter((F.col("hire_date") >= F.lit("2023-03-23").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "job_title_level",
        "resolved_job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.upper("full_name").alias("full_name_upper"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title_2"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.manager_id",
        "e.job_title",
        "e.hire_date",
        "e.resolved_job_title",
        "e.is_remote_flag",
        F.col("d.email").alias("employee_email"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "manager_id",
        "hire_date",
        "resolved_job_title",
        "is_remote_flag",
        "employee_email",
        F.min("is_remote_flag").over(w_grp).alias("running_min_is_remote_flag"),
        F.sum("is_remote_flag").over(w_grp).alias("running_sum_is_remote_flag"),
        F.count("is_remote_flag").over(w_grp).alias("running_count_is_remote_flag"),
        F.dense_rank().over(w_running).alias("is_remote_flag_dense_rank"),
    )
)

stats = (
    metrics
    .rollup(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("running_count_is_remote_flag", [0.25, 0.5, 0.75]).alias("running_count_is_remote_flag_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_count_is_remote_flag"), 2).alias("avg_running_count_is_remote_flag"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    stats
    .select(
        "week_start",
        "year_start",
        "distinct_ids",
        "avg_running_count_is_remote_flag",
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
