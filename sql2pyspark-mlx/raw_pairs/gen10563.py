from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("location").isin(["London", "Tokyo", "Bangkok"]))
    .select(
        "*",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.substring("location", 1, 4).alias("location_prefix"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

with_windows = (
    valid
    .withColumn("running_count_budget", F.count("budget").over(w_ordered))
    .withColumn("running_avg_budget", F.avg("budget").over(w_ordered))
    .withColumn("running_max_budget", F.max("budget").over(w_ordered))
    .withColumn("running_min_budget", F.min("budget").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.salary"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.employee_hire_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.dept_name_prefix",
        "s.resolved_dept_name",
        "s.running_count_budget",
        "s.running_avg_budget",
        "s.running_max_budget",
        "s.running_min_budget",
        "s.seq_no",
        "s.employee_hire_date",
        F.col("c.job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

result = (
    latest_match
    .groupBy("job_title")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.min("running_count_budget").alias("min_running_count_budget"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .orderBy(F.col("min_running_count_budget").desc_nulls_first())
)
