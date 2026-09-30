from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("next_budget", "id")
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.partitionBy("dept_name").orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("budget").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("budget").isNull())
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([35, 36])), "left_anti")
)

with_windows = (
    eligible
    .withColumn("next_budget", F.lead("budget").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

scored = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_prefix",
        "next_budget",
        F.lead("budget").over(w2).alias("next_budget_2"),
        F.lead("event_seq").over(w2).alias("next_event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w3)), 4).alias("event_seq_share"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.year(F.col("x.hire_date")) == 2025), "left_semi")
)

with_windows_departments = (
    not_flagged
    .withColumn("running_min_next_budget_2", F.min("next_budget_2").over(w4))
    .withColumn("running_sum_next_budget_2", F.sum("next_budget_2").over(w4))
    .withColumn("running_count_next_budget_2", F.count("next_budget_2").over(w4))
    .withColumn("budget_bucket", F.ntile(10).over(w5))
)

latest_match = (
    with_windows_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.next_budget",
        "s.next_budget_2",
        "s.next_event_seq",
        "s.event_seq_share",
        "s.running_min_next_budget_2",
        "s.running_sum_next_budget_2",
        "s.running_count_next_budget_2",
        "s.budget_bucket",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "next_budget",
        "next_budget_2",
        "next_event_seq",
        "event_seq_share",
        "running_min_next_budget_2",
        "running_sum_next_budget_2",
        "running_count_next_budget_2",
        "employee_job_title",
        F.when(F.col("employee_job_title").rlike("^(Manager|Analyst)$"), "PRIORITY").when(F.col("employee_job_title").rlike("^(Engineer|Director)$"), "CORE").otherwise("GROUP_B").alias("employee_job_title_zone"),
    )
    .orderBy("running_sum_next_budget_2")
)
