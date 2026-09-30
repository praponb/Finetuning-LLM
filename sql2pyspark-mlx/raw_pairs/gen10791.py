from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "hire_date"), "dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w_recent = Window.partitionBy(F.year("hire_date")).orderBy("budget", "id")
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_name").orderBy(F.col("department_budget").desc(), "id")
w5 = Window.partitionBy(F.month("hire_date"), "dept_id").orderBy("budget_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("prev_budget", "id")
w_grp = Window.partitionBy("job_title", "dept_name").orderBy("budget_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("department_location", "level").orderBy("prev_prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("department_dept_name", "dept_id").orderBy("running_avg_budget_avg_3", "id")

enriched = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") == "Bangkok")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.job_title",
        "s.hire_date",
        "s.is_remote",
        "s.level",
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)

enriched_employees = (
    enriched
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_3"),
        F.lag("budget", 2).over(w_recent).alias("prev_budget"),
    )
)

joined = (
    enriched_employees
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.budget").alias("department_budget"),
        F.col("b.location").alias("department_location"),
        F.col("b.dept_name").alias("department_dept_name"),
    )
)

with_windows = (
    joined
    .withColumn("job_title_avg_budget", F.avg("budget").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("first_known_department_dept_name", F.first("department_dept_name", ignorenulls=True).over(w5))
    .withColumn("prev_prev_budget", F.lag("prev_budget", 2).over(w6))
)

enriched_employees_2 = (
    with_windows
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "level",
        "budget",
        "prev_budget",
        "department_location",
        "department_dept_name",
        "job_title_avg_budget",
        "prev_prev_budget",
        F.max("budget_avg_3").over(w_grp).alias("running_max_budget_avg_3"),
        F.sum("budget_avg_3").over(w_grp).alias("running_sum_budget_avg_3"),
        F.avg("budget_avg_3").over(w_grp).alias("running_avg_budget_avg_3"),
    )
)

with_windows_employees = (
    enriched_employees_2
    .select(
        "id",
        "dept_id",
        "job_title",
        "level",
        "prev_budget",
        "department_dept_name",
        "job_title_avg_budget",
        "running_max_budget_avg_3",
        "running_sum_budget_avg_3",
        F.count("running_sum_budget_avg_3").over(w8).alias("running_count_running_sum_budget_avg_3"),
        F.max("running_sum_budget_avg_3").over(w8).alias("running_max_running_sum_budget_avg_3"),
        F.avg("running_sum_budget_avg_3").over(w8).alias("running_avg_running_sum_budget_avg_3"),
        F.lag("job_title_avg_budget", 2).over(w9).alias("prev_job_title_avg_budget"),
    )
)

summary = (
    with_windows_employees
    .rollup("department_dept_name")
    .agg(
        F.percentile_approx("running_max_budget_avg_3", 0.95).alias("running_max_budget_avg_3_p95"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("prev_budget", 0.9).alias("prev_budget_p90"),
        F.round(F.sum("running_avg_running_sum_budget_avg_3") / F.nullif(F.count(F.when(F.col("job_title") == "Director", 1)), F.lit(0)), 2).alias("running_avg_running_sum_budget_avg_3_per_director"),
    )
)

result = (
    summary
    .select(
        "department_dept_name",
        "job_title_list",
        "running_avg_running_sum_budget_avg_3_per_director",
        F.when(F.col("department_dept_name").rlike("^(Marketing|HR)$"), "PRIORITY").when(F.col("department_dept_name").rlike("^(Engineering|Sales)$"), "EXTENDED").otherwise("CORE").alias("department_dept_name_zone"),
    )
)
