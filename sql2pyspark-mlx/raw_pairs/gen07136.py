from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_scaled").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w4 = Window.orderBy("budget", "id")
w_recent = Window.partitionBy("job_title").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("employee_hire_date", "id")
w_grp = Window.partitionBy("dept_name").orderBy("prev_budget", "id")

cleaned = (
    df_departments
    .filter(F.col("budget") > 2702000)
    .select("*", F.round((F.col("budget") * 1.15), 2).alias("budget_scaled"))
)

deduped = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.budget_scaled",
        "s.row_num",
        F.col("c.level"),
    )
)

with_latest_departments = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        "s.row_num",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title"),
    )
)

scored = (
    with_latest_departments
    .filter(F.col("budget") >= 3701000)
    .select(
        "id",
        "dept_name",
        "budget",
        "employee_hire_date",
        F.lag("budget", 2).over(w4).alias("prev_budget"),
        F.sum("budget").over(w_recent).alias("cumulative_budget"),
        F.lag("budget", 1).over(w6).alias("prev_budget_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "dept_name",
        "budget",
        "employee_hire_date",
        "prev_budget_2",
        F.lag("budget").over(w_grp).alias("prev_budget_3"),
        F.lag("prev_budget_2").over(w_grp).alias("prev_prev_budget_2"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
