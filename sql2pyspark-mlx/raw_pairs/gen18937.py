from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("employee_level", "dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last7_max_salary", "id").rowsBetween(-5, Window.currentRow)

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level").isNotNull()), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        F.col("c.salary"),
        F.col("c.level").alias("employee_level"),
    )
)

with_windows = (
    with_latest
    .filter(F.col("employee_level") == "L4")
    .select(
        "*",
        F.max("salary").over(w_hist).alias("last7_max_salary"),
        F.avg("salary").over(w_hist).alias("last7_avg_salary"),
        F.sum("salary").over(w_hist).alias("last7_sum_salary"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "salary",
        "last7_avg_salary",
        F.sum("last7_avg_salary").over(w3).alias("running_sum_last7_avg_salary"),
        F.min("last7_avg_salary").over(w3).alias("running_min_last7_avg_salary"),
        F.count("last7_avg_salary").over(w3).alias("running_count_last7_avg_salary"),
        F.avg("last7_max_salary").over(w4).alias("last7_max_salary_avg_5"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isNotNull()), "left_anti")
)

totals = (
    not_flagged
    .groupBy(
        F.when(F.col("running_min_last7_avg_salary") >= 151000, "PREMIUM").otherwise("STANDARD").alias("running_min_last7_avg_salary_tier"),
        F.when(F.col("last7_avg_salary") >= 122000, "HIGH").otherwise("LOW").alias("last7_avg_salary_tier"),
        F.when(F.col("salary") >= 154000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.avg("last7_max_salary_avg_5"), 2).alias("avg_last7_max_salary_avg_5"),
    )
)

result = (
    totals
    .select(
        "running_min_last7_avg_salary_tier",
        "last7_avg_salary_tier",
        "salary_tier",
        "distinct_ids",
        "avg_salary",
    )
)
