from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("employee_level").orderBy("budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("employee_level").orderBy("budget_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("employee_level", "employee_job_title").orderBy(F.col("budget_bucket").desc())

filtered = (
    df_departments
    .filter(F.col("budget") < 1164000)
    .select("*", F.substring("location", 1, 4).alias("location_prefix"))
)

with_last = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.location_prefix",
        F.col("c.level").alias("employee_level"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L1", "L5", "L2"])), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "location_prefix",
        F.ntile(10).over(w2).alias("budget_bucket"),
        F.ntile(5).over(w3).alias("budget_bucket_2"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

metrics = (
    with_latest
    .select(
        "id",
        "location_prefix",
        "budget_bucket",
        "budget_bucket_2",
        "employee_job_title",
        "employee_level",
        F.sum("budget_bucket").over(w5).alias("running_sum_budget_bucket"),
        F.min("budget_bucket").over(w5).alias("running_min_budget_bucket"),
        F.count("budget_bucket").over(w5).alias("running_count_budget_bucket"),
        F.avg("budget_bucket").over(w5).alias("running_avg_budget_bucket"),
        F.dense_rank().over(w6).alias("budget_bucket_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "location_prefix",
        "budget_bucket_2",
        "employee_job_title",
        "employee_level",
        "running_sum_budget_bucket",
        "running_min_budget_bucket",
        "running_count_budget_bucket",
        "budget_bucket_dense_rank",
    )
    .orderBy("running_count_budget_bucket")
)
