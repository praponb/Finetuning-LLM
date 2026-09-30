from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("location", "level").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("budget") <= 1809000)
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

filtered = (
    base
    .filter(F.col("location") == "Singapore")
    .select(
        "id",
        "location",
        "budget",
        "dept_name_location",
        "dept_name_clean",
        F.trim("dept_name").alias("clean_dept_name"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_filled"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"), F.col("c.level"))
)

with_windows = (
    latest_match
    .withColumn("last4_avg_budget", F.avg("budget").over(w_part))
    .withColumn("last4_max_budget", F.max("budget").over(w_part))
    .withColumn("last4_min_budget", F.min("budget").over(w_part))
    .withColumn("seq_no", F.row_number().over(w3))
)

latest_match_departments = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.employee_hire_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name_location",
        "s.dept_name_clean",
        "s.dept_name_filled",
        "s.employee_hire_date",
        "s.level",
        "s.last4_avg_budget",
        "s.last4_max_budget",
        "s.last4_min_budget",
        F.col("c.salary"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match_departments
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.sum(F.when(F.col("level") == "L2", F.col("last4_avg_budget")).otherwise(0)).alias("l2_last4_avg_budget"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("l5_rows").desc_nulls_first())
)
