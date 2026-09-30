from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("budget").asc_nulls_last(), "id")
w3 = Window.partitionBy("job_title")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("budget").desc())
w6 = Window.orderBy("rank_in_group", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("job_title_min_budget")
w8 = Window.orderBy("budget_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("budget_percent_rank", "id")
w10 = Window.orderBy(F.col("job_title_count_salary").desc(), "id")
w11 = Window.orderBy("job_title_count_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        F.col("d.budget"),
    )
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "full_name", "job_title", "salary", "budget")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "full_name",
        "budget",
        "rank_in_group",
        F.count("salary").over(w3).alias("job_title_count_salary"),
        F.min("budget").over(w3).alias("job_title_min_budget"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.sum("job_title_count_salary").over(w4).alias("running_job_title_count_salary"),
        F.percent_rank().over(w5).alias("budget_percent_rank"),
    )
)

scored_employees = (
    scored
    .filter(F.col("running_job_title_count_salary") != 48)
    .select(
        "id",
        "full_name",
        "job_title_count_salary",
        F.avg("job_title_min_budget").over(w6).alias("job_title_min_budget_avg_6"),
        F.rank().over(w7).alias("job_title_min_budget_rank"),
        F.first("budget_percent_rank", ignorenulls=True).over(w8).alias("first_known_budget_percent_rank"),
        F.lag("budget", 2, 0).over(w9).alias("prev_budget"),
    )
)

metrics_employees = (
    scored_employees
    .select(
        "id",
        "full_name",
        "job_title_count_salary",
        "job_title_min_budget_avg_6",
        "job_title_min_budget_rank",
        "first_known_budget_percent_rank",
        F.row_number().over(w10).alias("row_seq"),
        F.sum("job_title_min_budget_rank").over(w11).alias("cumulative_job_title_min_budget_rank"),
    )
)

result = (
    metrics_employees
    .select("id", "full_name", "job_title_count_salary", "row_seq")
    .orderBy(F.col("job_title_count_salary").desc_nulls_first())
)
