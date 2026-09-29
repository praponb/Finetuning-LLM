from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy("latest_rank", "id")
w3 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("manager_id").orderBy("latest_rank", "id")
w6 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w7 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("level").orderBy("salary_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("job_title").orderBy("salary_dense_rank", "id").rowsBetween(-3, -1)
w10 = Window.partitionBy("level")
w11 = Window.partitionBy("level").orderBy(F.col("prev_salary_2").desc_nulls_last())
w12 = Window.partitionBy("level").orderBy(F.col("cumulative_salary_dense_rank").desc_nulls_last())
w13 = Window.partitionBy("level").orderBy("salary_rank_avg_3_prior", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-22").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.trim("job_title").alias("clean_job_title"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.lag("salary", 2).over(w2).alias("prev_salary"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
        F.dense_rank().over(w4).alias("salary_dense_rank"),
        F.lag("salary", 1, 0).over(w5).alias("prev_salary_2"),
        F.rank().over(w6).alias("salary_rank"),
    )
)

metrics = (
    scored
    .filter(F.col("salary_dense_rank") != 4)
    .select(
        "id",
        "level",
        "clean_job_title",
        "resolved_job_title",
        "prev_salary_2",
        F.last("job_title", ignorenulls=True).over(w7).alias("last_known_job_title"),
        F.sum("salary_dense_rank").over(w8).alias("cumulative_salary_dense_rank"),
        F.avg("salary_rank").over(w_recent).alias("salary_rank_avg_3_prior"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "level",
        "clean_job_title",
        "resolved_job_title",
        "cumulative_salary_dense_rank",
        F.round((F.col("prev_salary_2") / F.sum("prev_salary_2").over(w10)), 4).alias("prev_salary_2_share"),
        F.cume_dist().over(w11).alias("prev_salary_2_cume_dist"),
        F.cume_dist().over(w12).alias("cumulative_salary_dense_rank_cume_dist"),
        F.avg("salary_rank_avg_3_prior").over(w13).alias("salary_rank_avg_3_prior_avg_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "level",
        "clean_job_title",
        "resolved_job_title",
        "cumulative_salary_dense_rank",
        "prev_salary_2_share",
        F.when(F.col("level").rlike("^(L1|L5)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("level_zone"),
    )
)
