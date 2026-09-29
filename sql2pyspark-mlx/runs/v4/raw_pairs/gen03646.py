from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w_ordered = Window.orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w7 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("dept_name").orderBy("last_known_budget", "id")
w9 = Window.partitionBy("job_title").orderBy(F.col("budget_avg_2").desc(), "id")
w10 = Window.partitionBy("job_title", "dept_name").orderBy(F.col("prev_budget").asc_nulls_last())
w11 = Window.partitionBy("dept_name").orderBy("budget_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("location", "dept_name").orderBy(F.col("next_budget_avg_2").desc(), "id")

scored = (
    df_departments
    .select(
        "*",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.ntile(10).over(w_ordered).alias("budget_bucket"),
        F.avg("budget").over(w_recent).alias("budget_avg_2_prior"),
        F.avg("budget").over(w4).alias("budget_avg_2"),
        F.last("budget", ignorenulls=True).over(w5).alias("last_known_budget"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.job_title"))
)

with_windows = (
    with_last
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w7))
    .withColumn("next_budget_avg_2", F.lead("budget_avg_2", 2).over(w8))
    .withColumn("row_seq", F.row_number().over(w9))
    .withColumn("prev_budget_cume_dist", F.cume_dist().over(w10))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w11))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "location",
        "budget",
        "prev_budget",
        "budget_bucket",
        "budget_avg_2_prior",
        "budget_avg_2",
        "job_title",
        "first_known_location",
    )
    .orderBy(F.col("budget_avg_2_prior").asc_nulls_first(), "prev_budget")
)
