from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

deduped = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.hire_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title") == "Intern"), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id") == 31), "left_anti")
)

result = (
    kept
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .rollup("year_start", "dept_name")
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count("*").alias("event_count"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy("year_start")
)
