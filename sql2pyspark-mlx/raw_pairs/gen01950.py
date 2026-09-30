from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("full_name_length").asc_nulls_last())
w2 = Window.partitionBy("job_title", "dept_id")
w3 = Window.orderBy("salary")
w4 = Window.partitionBy("job_title").orderBy(F.col("full_name_length").desc_nulls_first())
w5 = Window.partitionBy("job_title")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-08-03").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.length("full_name").alias("full_name_length"),
    )
)

kept_keys = (
    cleaned
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("dept_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

cleaned_employees = (
    kept_rows
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "is_remote_label",
        "full_name_length",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)

with_windows = (
    cleaned_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "is_remote_label",
        "full_name_length",
        F.rank().over(w).alias("full_name_length_rank"),
        F.stddev_samp("salary").over(w2).alias("job_title_stddev_samp_salary"),
        F.rank().over(w3).alias("salary_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.greatest("full_name_length_rank", "full_name_length").alias("greatest_full_name_length_rank_full_name_length"),
        F.regexp_replace("full_name", " ", "-").alias("full_name_slug"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.cume_dist().over(w4).alias("full_name_length_cume_dist"),
        F.stddev_samp("greatest_full_name_length_rank_full_name_length").over(w5).alias("job_title_stddev_samp_greatest_full_name_length_rank_full_name_length"),
        F.round((F.col("job_title_stddev_samp_salary") / F.sum("job_title_stddev_samp_salary").over(w5)), 4).alias("job_title_stddev_samp_salary_share"),
    )
)

totals = (
    metrics
    .rollup(
        F.when(F.col("job_title_stddev_samp_greatest_full_name_length_rank_full_name_length") >= 81, "PREMIUM").otherwise("STANDARD").alias("job_title_stddev_samp_greatest_full_name_length_rank_full_name_length_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
    )
    .filter(F.col("engineer_rows") > 1)
)

result = (
    totals
    .select(
        "job_title_stddev_samp_greatest_full_name_length_rank_full_name_length_tier",
        "activity_month",
        "distinct_job_titles",
    )
    .orderBy(F.col("distinct_job_titles").desc_nulls_first())
)
