from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id")
w3 = Window.orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w5 = Window.partitionBy(F.month("hire_date"), "manager_id").orderBy("salary", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("dept_name", "job_title").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("job_title").orderBy(F.col("salary_share").desc(), "id")
w9 = Window.partitionBy("dept_id").orderBy("salary_share", "id")
w10 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("cumulative_salary_share", "id")
w12 = Window.partitionBy("job_title").orderBy("row_seq_bucket", "id")
w13 = Window.partitionBy("job_title").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("job_title")
w15 = Window.partitionBy("dept_id").orderBy(F.col("salary_share_bucket_2").desc_nulls_first(), "id")

cleaned = (
    df_employees
    .filter(F.col("job_title") != "Intern")
    .withColumn("email_filled", F.coalesce("email", F.lit("Unknown")))
    .withColumn("level_filled", F.coalesce("level", F.lit("N/A")))
)

metrics = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "level_filled",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.avg("salary").over(w3).alias("salary_avg_6"),
        F.row_number().over(w4).alias("seq_no"),
        F.avg("salary").over(w5).alias("salary_avg_6_prior"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.job_title",
        "e.level_filled",
        "e.row_seq",
        "e.salary_share",
        "e.salary_avg_6",
        "e.seq_no",
        "e.salary_avg_6_prior",
        F.col("b.dept_name"),
        F.col("b.location"),
        F.col("b.budget"),
    )
)

enriched = (
    joined
    .filter(F.col("seq_no") < 8)
    .select(
        "*",
        F.sum("salary_share").over(w6).alias("cumulative_salary_share"),
        F.ntile(3).over(w7).alias("row_seq_bucket"),
        F.ntile(4).over(w8).alias("salary_share_bucket"),
        F.ntile(5).over(w9).alias("salary_share_bucket_2"),
        F.sum("budget").over(w10).alias("cumulative_budget"),
    )
)

with_windows = (
    enriched
    .withColumn("seq_no_2", F.row_number().over(w_ordered))
    .withColumn("prev_cumulative_salary_share", F.lag("cumulative_salary_share", 2).over(w12))
)

enriched_employees = (
    with_windows
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary_avg_6",
        "location",
        "salary_share_bucket",
        "salary_share_bucket_2",
        "cumulative_budget",
        "seq_no_2",
        "prev_cumulative_salary_share",
        F.count("salary_avg_6_prior").over(w13).alias("running_count_salary_avg_6_prior"),
        F.min("salary_avg_6_prior").over(w13).alias("running_min_salary_avg_6_prior"),
        F.max("salary_avg_6_prior").over(w13).alias("running_max_salary_avg_6_prior"),
        F.sum("salary_avg_6_prior").over(w13).alias("running_sum_salary_avg_6_prior"),
        F.round((F.col("salary_avg_6") / F.sum("salary_avg_6").over(w14)), 4).alias("salary_avg_6_share"),
    )
)

latest = (
    enriched_employees
    .withColumn("rank_in_group", F.row_number().over(w15))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count_if(F.col("location") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum("running_min_salary_avg_6_prior").alias("total_running_min_salary_avg_6_prior"),
    )
    .orderBy(F.col("chiang_mai_events").asc_nulls_first())
)
