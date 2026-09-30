from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("running_sum_salary", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("is_remote_flag_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"))
w6 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.year("hire_date_minus_90d"), "level")

valid = (
    df_employees
    .filter(F.col("full_name").isNull())
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

filtered = (
    valid
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-06-24"))) & (F.col("hire_date") <= "2025-06-14"))
    .select("*", F.date_sub("hire_date", 90).alias("hire_date_minus_90d"))
)

enriched = (
    filtered
    .filter(F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "level",
        "is_remote_flag",
        "hire_date_minus_90d",
        F.avg("salary").over(w_part).alias("running_avg_salary"),
        F.max("salary").over(w_part).alias("running_max_salary"),
        F.sum("salary").over(w_part).alias("running_sum_salary"),
        F.count("salary").over(w_part).alias("running_count_salary"),
        F.avg("is_remote_flag").over(w2).alias("is_remote_flag_avg_5_prior"),
    )
)

enriched_employees = (
    enriched
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "is_remote_flag",
        "running_sum_salary",
        "running_count_salary",
        "is_remote_flag_avg_5_prior",
        F.avg("is_remote_flag_avg_5_prior").over(w3).alias("is_remote_flag_avg_5_prior_avg_5_prior"),
        F.first("is_remote_flag_avg_5_prior", ignorenulls=True).over(w4).alias("first_known_is_remote_flag_avg_5_prior"),
        F.count("salary").over(w5).alias("hire_date_period_count_salary"),
        F.sum("running_sum_salary").over(w6).alias("cumulative_running_sum_salary"),
        F.stddev_samp("running_max_salary").over(w7).alias("hire_date_minus_90d_year_stddev_samp_running_max_salary"),
    )
)

base = (
    enriched_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-08-29")))
    .select(
        "id",
        "full_name",
        "hire_date",
        "is_remote_flag",
        "running_count_salary",
        "is_remote_flag_avg_5_prior",
        "is_remote_flag_avg_5_prior_avg_5_prior",
        "first_known_is_remote_flag_avg_5_prior",
        "hire_date_minus_90d_year_stddev_samp_running_max_salary",
        F.lower("full_name").alias("full_name_lower"),
    )
)

filtered_employees = (
    base
    .filter(F.col("first_known_is_remote_flag_avg_5_prior").between(62, 74))
    .select("*", F.floor("running_count_salary").alias("running_count_salary_whole"))
)

remaining_keys = (
    filtered_employees
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.col("dept_id").isin([11, 20, 35, 39]))
        .select("id")
    )
)

reconciled = (
    filtered_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("is_remote_flag_avg_5_prior_avg_5_prior") >= 85, "LARGE").otherwise("SMALL").alias("is_remote_flag_avg_5_prior_avg_5_prior_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("running_count_salary_whole", [0.25, 0.5, 0.75]).alias("running_count_salary_whole_quartiles"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_count_salary_whole", [0.25, 0.5, 0.75]).alias("running_count_salary_whole_quartiles_2"),
        F.round(F.avg("running_count_salary"), 2).alias("avg_running_count_salary"),
    )
)
