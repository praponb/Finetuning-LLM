from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("level_length", "id")
w3 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("row_seq").desc())
w_part = Window.partitionBy("dept_id").orderBy("row_seq", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("job_title")
w6 = Window.orderBy("last3_min_next_level_length", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("dept_id").orderBy(F.col("next_level_length").desc_nulls_last())
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("job_title_stddev_samp_row_seq", "id")
w10 = Window.partitionBy("level").orderBy("job_title_stddev_samp_row_seq", "id")
w11 = Window.partitionBy("level").orderBy(F.col("next_level_length_rank").desc(), "id")
w12 = Window.partitionBy("location").orderBy("last3_count_next_level_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= "2025-04-01"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.length("level").alias("level_length"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("level_length", 1, 0).over(w2).alias("next_level_length"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .withColumn("last3_min_next_level_length", F.min("next_level_length").over(w_part))
    .withColumn("last3_sum_next_level_length", F.sum("next_level_length").over(w_part))
    .withColumn("last3_count_next_level_length", F.count("next_level_length").over(w_part))
    .withColumn("last3_avg_next_level_length", F.avg("next_level_length").over(w_part))
    .withColumn("job_title_stddev_samp_row_seq", F.stddev_samp("row_seq").over(w_hist))
)

scored = (
    with_windows
    .withColumn("last3_count_next_level_length_avg_6_prior", F.avg("last3_count_next_level_length").over(w6))
    .withColumn("next_level_length_rank", F.rank().over(w7))
    .withColumn("cumulative_last3_avg_next_level_length", F.sum("last3_avg_next_level_length").over(w8))
    .withColumn("job_title_stddev_samp_row_seq_bucket", F.ntile(3).over(w9))
)

enriched_employees = (
    scored
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.hire_date",
        "a.is_remote",
        "a.level",
        "a.level_length",
        "a.job_title_filled",
        "a.last3_sum_next_level_length",
        "a.last3_count_next_level_length",
        "a.job_title_stddev_samp_row_seq",
        "a.next_level_length_rank",
        F.col("d.location"),
    )
)

metrics = (
    enriched_employees
    .select(
        "*",
        F.lag("last3_sum_next_level_length", 1, 0).over(w10).alias("prev_last3_sum_next_level_length"),
        F.row_number().over(w11).alias("row_seq"),
        F.first("level", ignorenulls=True).over(w12).alias("first_known_level"),
    )
)

result = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.dept_name").alias("dept_name"),
        F.col("d.location").alias("department_location"),
        F.col("e.level").alias("level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.next_level_length_rank")).alias("min_next_level_length_rank"),
        F.round(F.sum(F.col("e.job_title_stddev_samp_row_seq")), 2).alias("total_job_title_stddev_samp_row_seq"),
    )
)
