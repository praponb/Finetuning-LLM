from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("adjusted_salary", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy(F.col("adjusted_salary").desc(), "id")
w_running = Window.partitionBy("job_title").orderBy("adjusted_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("job_title").orderBy(F.col("running_adjusted_salary").desc_nulls_last())
w6 = Window.partitionBy("job_title").orderBy(F.col("adjusted_salary_avg_5").asc_nulls_last(), "id")
w7 = Window.partitionBy("adjusted_salary_bucket_times_adjusted_salary_avg_5_tier").orderBy(F.col("adjusted_salary_bucket_p90").desc())

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-22"))) & F.col("dept_id").isin([3, 15, 35]))
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        "is_remote",
        F.round((F.col("salary") * 0.85), 2).alias("adjusted_salary"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "job_title",
        "level_clean",
        F.avg("adjusted_salary").over(w).alias("adjusted_salary_avg_5"),
        F.ntile(4).over(w2).alias("adjusted_salary_bucket"),
        F.sum("adjusted_salary").over(w_running).alias("running_adjusted_salary"),
        F.avg("adjusted_salary").over(w4).alias("adjusted_salary_avg_2_prior"),
    )
)

valid_employees = (
    metrics
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("adjusted_salary_bucket") >= 7, "High").when(F.col("adjusted_salary_bucket") >= 2, "Medium").otherwise("Low").alias("adjusted_salary_bucket_tier"),
        (F.col("adjusted_salary_bucket") * F.col("adjusted_salary_avg_5")).alias("adjusted_salary_bucket_times_adjusted_salary_avg_5"),
    )
)

deduped = (
    valid_employees
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "adjusted_salary_bucket",
        "adjusted_salary_avg_2_prior",
        "adjusted_salary_bucket_tier",
        "adjusted_salary_bucket_times_adjusted_salary_avg_5",
    )
)

result = (
    ranked
    .groupBy(
        F.when(F.col("adjusted_salary_bucket_times_adjusted_salary_avg_5") >= 88, "LARGE").otherwise("SMALL").alias("adjusted_salary_bucket_times_adjusted_salary_avg_5_tier"),
        F.when(F.col("adjusted_salary_bucket") >= 8, "LARGE").otherwise("SMALL").alias("adjusted_salary_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("adjusted_salary_bucket", 0.9).alias("adjusted_salary_bucket_p90"),
    )
    .filter(F.col("adjusted_salary_bucket_p90") > 0)
    .withColumn("adjusted_salary_bucket_p90_dense_rank", F.dense_rank().over(w7))
    .orderBy(F.col("distinct_ids").desc())
)
