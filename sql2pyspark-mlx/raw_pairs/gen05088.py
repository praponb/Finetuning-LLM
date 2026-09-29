from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.month("hire_date")).orderBy("hire_date", "id")
w3 = Window.partitionBy("level").orderBy("hire_date_plus_months", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.partitionBy("manager_id").orderBy("salary", "id")
w5 = Window.partitionBy("job_title", "dept_id")
w6 = Window.partitionBy("manager_id").orderBy("salary_avg_3", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("level").orderBy("prev_salary", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("job_title", "manager_id").orderBy("last7_count_running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-09").cast("date")) & (~F.col("job_title").eqNullSafe(F.lit("Analyst"))))
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
        F.add_months("hire_date", 6).alias("hire_date_plus_months"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

metrics = (
    valid
    .filter(F.col("hire_date_plus_months") <= "2024-01-20")
    .select(
        "*",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("salary").over(w3).alias("salary_avg_3"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("salary_avg_3").over(w_part).alias("next_salary_avg_3"),
        F.lag("salary_avg_3").over(w_part).alias("prev_salary_avg_3"),
        F.lag("salary").over(w_part).alias("prev_salary"),
        F.lead("salary").over(w_part).alias("next_salary"),
        F.row_number().over(w_part).alias("seq_no"),
        F.min("running_salary").over(w5).alias("job_title_min_running_salary"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.avg("running_salary").over(w6).alias("last7_avg_running_salary"),
        F.min("running_salary").over(w6).alias("last7_min_running_salary"),
        F.count("running_salary").over(w6).alias("last7_count_running_salary"),
        F.sum("running_salary").over(w6).alias("last7_sum_running_salary"),
        F.avg("next_salary").over(w7).alias("next_salary_avg_5_prior"),
    )
)

scored_employees = (
    with_windows
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "resolved_level",
        "running_salary",
        "row_seq",
        "salary_avg_3",
        "next_salary_avg_3",
        "next_salary",
        "seq_no",
        "last7_avg_running_salary",
        "last7_min_running_salary",
        "next_salary_avg_5_prior",
        F.min("prev_salary").over(w_running).alias("running_min_prev_salary"),
        F.sum("prev_salary").over(w_running).alias("running_sum_prev_salary"),
        F.avg("prev_salary").over(w_running).alias("running_avg_prev_salary"),
    )
)

stats = (
    scored_employees
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .withColumn("salary_avg_3_tier", F.when(F.col("salary_avg_3") >= 144198, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_month", "salary_avg_3_tier"], ["activity_month"]],
        "activity_month",
        "salary_avg_3_tier",
    )
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("level") == "L4", 1)), F.lit(0)), 2).alias("seq_no_per_l4"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.percentile_approx("last7_avg_running_salary", [0.25, 0.5, 0.75]).alias("last7_avg_running_salary_quartiles"),
        F.percentile_approx("running_avg_prev_salary", 0.95).alias("running_avg_prev_salary_p95"),
    )
    .filter(F.col("running_avg_prev_salary_p95") < 1000000)
)

result = (
    stats
    .select(
        "activity_month",
        "salary_avg_3_tier",
        "seq_no_per_l4",
        "l3_count",
        "last7_avg_running_salary_quartiles",
        "running_avg_prev_salary_p95",
    )
    .orderBy(F.col("l3_count").desc())
)
