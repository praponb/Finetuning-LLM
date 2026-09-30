from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("hire_date"), "job_title").orderBy("running_avg_salary")
w6 = Window.partitionBy("job_title").orderBy("running_sum_salary", "id")
w7 = Window.partitionBy(F.date_trunc("week", "hire_date")).orderBy(F.col("salary").desc(), "id")
w8 = Window.partitionBy("level")
w9 = Window.partitionBy("level").orderBy("running_sum_salary", "id")
w10 = Window.partitionBy("level").orderBy("running_min_salary", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("level").orderBy("last_known_running_sum_salary", "id").rowsBetween(-6, -1)

metrics = (
    df_employees
    .filter(F.col("level") == "L3")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.min("salary").over(w_recent).alias("running_min_salary"),
        F.count("salary").over(w_recent).alias("running_count_salary"),
        F.avg("salary").over(w_recent).alias("running_avg_salary"),
        F.sum("salary").over(w_recent).alias("running_sum_salary"),
        F.avg("salary").over(w_part).alias("salary_avg_6"),
    )
)

metrics_employees = (
    metrics
    .withColumn("last_known_running_sum_salary", F.last("running_sum_salary", ignorenulls=True).over(w_grp))
    .withColumn("first_known_running_avg_salary", F.first("running_avg_salary", ignorenulls=True).over(w4))
)

with_windows = (
    metrics_employees
    .withColumn("running_avg_salary_dense_rank", F.dense_rank().over(w5))
    .withColumn("next_salary", F.lead("salary", 1).over(w6))
)

cleaned = (
    with_windows
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-03-14"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        "level",
        "running_min_salary",
        "running_count_salary",
        "running_avg_salary",
        "running_sum_salary",
        "last_known_running_sum_salary",
        "running_avg_salary_dense_rank",
        F.round((F.col("running_avg_salary_dense_rank") * 0.9), 2).alias("adjusted_running_avg_salary_dense_rank"),
        F.trim("email").alias("clean_email"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "email",
        "hire_date",
        "running_min_salary",
        "running_count_salary",
        "running_avg_salary",
        "running_sum_salary",
        "running_avg_salary_dense_rank",
        "adjusted_running_avg_salary_dense_rank",
        "clean_email",
        F.row_number().over(w7).alias("seq_no"),
        F.max("running_avg_salary_dense_rank").over(w8).alias("level_max_running_avg_salary_dense_rank"),
        F.lead("last_known_running_sum_salary", 2, 0).over(w9).alias("next_last_known_running_sum_salary"),
        F.avg("running_avg_salary").over(w10).alias("running_avg_salary_avg_6_prior"),
        F.avg("running_min_salary").over(w11).alias("running_min_salary_avg_6_prior"),
    )
)

result = (
    scored
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date"))
    .withColumn("level_max_running_avg_salary_dense_rank_tier", F.when(F.col("level_max_running_avg_salary_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "level_max_running_avg_salary_dense_rank_tier", "seq_no_tier"], ["quarter_start", "level_max_running_avg_salary_dense_rank_tier"], ["quarter_start"]],
        "quarter_start",
        "level_max_running_avg_salary_dense_rank_tier",
        "seq_no_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("adjusted_running_avg_salary_dense_rank", [0.25, 0.5, 0.75]).alias("adjusted_running_avg_salary_dense_rank_quartiles"),
        F.round(F.avg("running_avg_salary_avg_6_prior"), 2).alias("avg_running_avg_salary_avg_6_prior"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
)
