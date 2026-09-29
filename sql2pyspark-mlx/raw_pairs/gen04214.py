from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("job_title").orderBy("salary", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("dept_name")
w6 = Window.partitionBy("dept_name").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_departments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.job_title"),
        F.col("c.salary"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date").between("2025-01-01", "2025-04-01")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "budget",
        "job_title",
        "salary",
        F.lead("budget", 2).over(w3).alias("next_budget"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("budget").over(w5).alias("dept_name_sum_budget"),
        F.sum("salary").over(w6).alias("cumulative_salary"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget",
        "job_title",
        "next_budget",
        "dept_name_sum_budget",
    )
    .orderBy(F.col("dept_name").desc())
)
