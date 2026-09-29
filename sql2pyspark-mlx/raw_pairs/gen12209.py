from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date"), "dept_id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w_part = Window.partitionBy("dept_id").orderBy("salary", "id")
w4 = Window.partitionBy("job_title", "level").orderBy(F.col("hire_date_month_count_salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > "2024-04-01")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
    )
)

base = (
    valid
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "email_filled",
        F.concat("job_title", F.lit(" - "), "full_name").alias("job_title_full_name"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.count("salary").over(w).alias("hire_date_month_count_salary"),
        F.cume_dist().over(w2).alias("salary_cume_dist"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "hire_date",
        "hire_date_month_count_salary",
        "salary_cume_dist",
        "row_seq",
    )
)

daily = (
    latest
    .groupBy(F.date_trunc("quarter", "hire_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("salary_cume_dist", 0.9).alias("salary_cume_dist_p90"),
        F.count("*").alias("event_count"),
        F.round(F.avg("salary_cume_dist"), 2).alias("avg_salary_cume_dist"),
    )
)

result = (
    daily
    .select(
        "quarter_start",
        "salary_cume_dist_p90",
        "event_count",
        "avg_salary_cume_dist",
    )
    .orderBy("salary_cume_dist_p90")
)
