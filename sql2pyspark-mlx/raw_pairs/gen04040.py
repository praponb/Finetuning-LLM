from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("salary").desc(), "id")
w_recent = Window.orderBy("seq_no_rounded", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-02-16").cast("date"))
    .withColumn("email_normalized", F.upper(F.trim("email")))
)

with_windows = (
    cleaned
    .filter(F.col("manager_id") == 6)
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "email_normalized",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(10).over(w2).alias("salary_bucket"),
    )
)

cleaned_employees = (
    with_windows
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-05-30")))
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        "email_normalized",
        "seq_no",
        "salary_bucket",
        F.regexp_replace("email", " ", "").alias("email_slug"),
    )
)

valid = (
    cleaned_employees
    .filter(F.col("salary_bucket").between(4, 7))
    .select(
        "id",
        "email",
        "hire_date",
        "email_normalized",
        "email_slug",
        F.round("salary_bucket", 2).alias("salary_bucket_rounded"),
        F.round("seq_no", 0).alias("seq_no_rounded"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.count("seq_no_rounded").over(w_recent).alias("last7_count_seq_no_rounded"),
        F.avg("seq_no_rounded").over(w_recent).alias("last7_avg_seq_no_rounded"),
        F.min("seq_no_rounded").over(w_recent).alias("last7_min_seq_no_rounded"),
        F.sum("salary_bucket_rounded").over(w_ordered).alias("cumulative_salary_bucket_rounded"),
    )
)

cleaned_employees_2 = (
    enriched
    .filter(F.col("cumulative_salary_bucket_rounded").isNull())
    .select(
        "id",
        "hire_date",
        "email_normalized",
        "seq_no_rounded",
        "last7_avg_seq_no_rounded",
        "last7_min_seq_no_rounded",
        (F.datediff(F.current_date(), "hire_date") / 7).alias("weeks_since_hire_date"),
    )
)

totals = (
    cleaned_employees_2
    .groupBy(
        F.when(F.col("last7_avg_seq_no_rounded") >= 84, "LARGE").otherwise("SMALL").alias("last7_avg_seq_no_rounded_tier"),
    )
    .agg(
        F.percentile_approx("weeks_since_hire_date", 0.95).alias("weeks_since_hire_date_p95"),
        F.percentile_approx("last7_min_seq_no_rounded", 0.5).alias("last7_min_seq_no_rounded_p50"),
        F.percentile_approx("last7_avg_seq_no_rounded", 0.9).alias("last7_avg_seq_no_rounded_p90"),
        F.percentile_approx("last7_avg_seq_no_rounded", [0.25, 0.5, 0.75]).alias("last7_avg_seq_no_rounded_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)

result = (
    totals
    .select(
        "last7_avg_seq_no_rounded_tier",
        "weeks_since_hire_date_p95",
        "last7_min_seq_no_rounded_p50",
        "last7_avg_seq_no_rounded_p90",
        "last7_avg_seq_no_rounded_quartiles",
        "distinct_ids",
        "distinct_ids_2",
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
