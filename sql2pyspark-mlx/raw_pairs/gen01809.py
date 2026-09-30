from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_job_title", "location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

base = (
    with_latest
    .filter(F.col("dept_name") == "Sales")
    .select(
        "id",
        "location",
        "budget",
        "employee_job_title",
        F.lower("location").alias("location_lower"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("budget").over(w2).alias("last3_avg_budget"),
        F.sum("budget").over(w2).alias("last3_sum_budget"),
        F.count("budget").over(w2).alias("last3_count_budget"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.employee_job_title",
        "s.last3_sum_budget",
        "s.last3_count_budget",
        F.col("c.job_title").alias("employee_job_title_2"),
    )
)

result = (
    with_last
    .groupBy("employee_job_title")
    .pivot("location", ["Tokyo", "Bangkok", "London"])
    .agg(F.sum("budget").alias("amount"), F.count(F.lit(1)).alias("n"))
)
