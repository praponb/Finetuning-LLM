from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_job_title").orderBy(F.col("budget").desc_nulls_last(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name") != "Sales")
    .select("*", F.round((F.col("budget") * 1.07), 2).alias("budget_scaled"))
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isNull()), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

valid_departments = (
    with_latest
    .filter(F.col("employee_job_title").isNotNull() & (F.col("employee_salary") >= 140000))
    .select(
        "*",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        (F.col("employee_salary") - F.col("budget")).alias("employee_salary_minus_budget"),
        F.coalesce("employee_job_title", F.lit("OTHER")).alias("employee_job_title_filled"),
    )
)

latest = (
    valid_departments
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "location",
        "employee_salary",
        "location_filled",
        "employee_job_title_filled",
        "row_num",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
