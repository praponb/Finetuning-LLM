from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w_grp = Window.partitionBy("level", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "level")
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-05-15").cast("date"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.coalesce("level", F.lit("Unknown")).alias("level_filled"),
        F.length("full_name").alias("full_name_length"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "salary",
        "is_remote",
        "level_filled",
        "full_name_length",
        "resolved_level",
        F.row_number().over(w).alias("event_seq"),
        F.sum("full_name_length").over(w_grp).alias("running_full_name_length"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.percent_rank().over(w4).alias("salary_percent_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("running_full_name_length") != 149)
    .select(
        "id",
        "salary",
        "level_filled",
        "full_name_length",
        "event_seq",
        "salary_share",
        "salary_percent_rank",
        F.ceil("running_full_name_length").alias("running_full_name_length_whole"),
    )
)

filtered_employees = (
    filtered
    .filter(F.col("salary_percent_rank") >= 0.41)
    .select(
        "id",
        "salary",
        "level_filled",
        "salary_share",
        "salary_percent_rank",
        "running_full_name_length_whole",
        (F.col("running_full_name_length_whole") - F.col("salary_share")).alias("running_full_name_length_whole_minus_salary_share"),
    )
)

result = (
    filtered_employees
    .groupBy(
        F.when(F.col("salary") >= 134000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count("*").alias("event_count"),
        F.percentile_approx("salary_share", 0.5).alias("salary_share_p50"),
        F.avg("salary_share").alias("avg_salary_share"),
    )
)
