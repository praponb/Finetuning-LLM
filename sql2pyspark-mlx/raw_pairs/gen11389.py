from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("department_budget").asc_nulls_first())
w2 = Window.partitionBy("manager_id", "department_location").orderBy("department_budget", "id")

filtered = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.upper("full_name").alias("full_name_upper"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.location"), F.col("d.budget"))
)

with_dims_employees = (
    with_dims
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.dept_name"),
        F.col("d.budget").alias("department_budget"),
        F.col("d.location").alias("department_location"),
    )
)

metrics = (
    with_dims_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "full_name_upper",
        "location",
        "dept_name",
        "department_budget",
        F.percent_rank().over(w_running).alias("department_budget_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("department_budget_percent_rank") >= 0.65, "HIGH").otherwise("LOW").alias("department_budget_percent_rank_tier"),
        "dept_name",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("department_budget_percent_rank") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("department_budget_percent_rank_per_marketing"),
        F.percentile_approx("department_budget", [0.25, 0.5, 0.75]).alias("department_budget_quartiles"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.percentile_approx("department_budget", 0.9).alias("department_budget_p90"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("location") == "New York", 1)), F.lit(0)), 2).alias("row_seq_per_new_york"),
    )
)

result = (
    daily
    .select(
        "department_budget_percent_rank_tier",
        "dept_name",
        "event_count",
        "department_budget_percent_rank_per_marketing",
        "department_budget_quartiles",
        "row_seq_per_new_york",
        F.when(F.col("dept_name").rlike("^(HR|Sales)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("dept_name_zone"),
    )
)
