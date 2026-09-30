from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("hire_date"), "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("level", "job_title")
w4 = Window.partitionBy(F.date_trunc("WEEK", "hire_date"), "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("week", "hire_date_period"), "manager_id").orderBy("salary", "id")
w6 = Window.orderBy("salary_share", "id").rowsBetween(-5, -1)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-22").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("level", F.lit("Unknown")).alias("level_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w).alias("last_known_level"),
        F.ntile(10).over(w_recent).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.sum("salary").over(w4).alias("running_salary"),
    )
)

base_employees = (
    with_windows
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-09-12")))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "level_filled",
        "last_known_level",
        "salary_share",
        "running_salary",
        F.date_trunc("month", "hire_date").alias("hire_date_period"),
    )
)

enriched = (
    base_employees
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .select(
        "e.id",
        "e.full_name",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.is_remote",
        "e.level",
        "e.level_filled",
        "e.last_known_level",
        "e.salary_share",
        "e.hire_date_period",
        F.col("p2.email").alias("employee_email"),
    )
)

metrics = (
    enriched
    .filter(F.col("level") != "L2")
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        "level_filled",
        "salary_share",
        F.lead("salary_share", 1).over(w5).alias("next_salary_share"),
        F.avg("salary").over(w6).alias("salary_avg_5_prior"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.manager_id",
        "a.level",
        "a.salary_share",
        F.col("e.job_title"),
        F.col("e.level").alias("employee_level"),
    )
)

result = (
    with_dims
    .select("id", "full_name", "level", "job_title", "employee_level")
    .orderBy("job_title")
)
