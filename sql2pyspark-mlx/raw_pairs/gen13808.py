from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("running_max_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy(F.col("running_avg_budget_avg_6").desc(), "id")
w6 = Window.partitionBy("dept_name")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") != "Sales"))
    .withColumn("location_lower", F.lower("location"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

metrics = (
    cleaned
    .select(
        "id",
        "dept_name",
        "budget",
        "location_lower",
        F.sum("budget").over(w_recent).alias("running_sum_budget"),
        F.min("budget").over(w_recent).alias("running_min_budget"),
        F.avg("budget").over(w_recent).alias("running_avg_budget"),
        F.max("budget").over(w_recent).alias("running_max_budget"),
        F.avg("budget").over(w2).alias("budget_avg_6"),
    )
)

metrics_departments = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget",
        "running_max_budget",
        F.max("budget_avg_6").over(w3).alias("running_max_budget_avg_6"),
        F.avg("budget_avg_6").over(w3).alias("running_avg_budget_avg_6"),
        F.min("budget_avg_6").over(w3).alias("running_min_budget_avg_6"),
        F.count("budget_avg_6").over(w3).alias("running_count_budget_avg_6"),
        F.sum("running_sum_budget").over(w4).alias("cumulative_running_sum_budget"),
    )
)

metrics_departments_2 = (
    metrics_departments
    .select(
        "id",
        "dept_name",
        "running_max_budget",
        "running_max_budget_avg_6",
        "running_min_budget_avg_6",
        "running_count_budget_avg_6",
        F.row_number().over(w5).alias("event_seq"),
        F.round((F.col("running_min_budget_avg_6") / F.sum("running_min_budget_avg_6").over(w6)), 4).alias("running_min_budget_avg_6_share"),
    )
)

not_flagged = (
    metrics_departments_2
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary").between(97000, 102000)), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

not_flagged_departments = (
    latest_match
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < F.col("s.employee_hire_date")), "left_anti")
)

summary = (
    not_flagged_departments
    .groupBy("dept_name")
    .agg(
        F.avg("running_max_budget").alias("avg_running_max_budget"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("event_seq_per_engineering"),
        F.count_if(F.col("dept_name") == "Finance").alias("finance_events"),
    )
)

result = (
    summary
    .select(
        "dept_name",
        "avg_running_max_budget",
        "event_seq_per_engineering",
        F.when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "GROUP_A").when(F.col("dept_name").rlike("^(Sales|HR)$"), "EXTENDED").otherwise("PRIORITY").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc(), F.col("event_seq_per_engineering").desc())
)
