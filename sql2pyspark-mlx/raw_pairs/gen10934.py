from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_ordered = Window.orderBy("budget", "id")
w3 = Window.partitionBy("employee_job_title").orderBy("budget", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("employee_job_title")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("budget_tier").orderBy(F.col("new_york_count").desc())

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title").isin(["Engineer", "Analyst"])), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

metrics = (
    with_latest
    .filter(F.col("dept_name") == "Engineering")
    .select(
        "*",
        F.ntile(3).over(w_ordered).alias("budget_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.avg("budget").over(w3).alias("budget_avg_6_prior"),
        F.count("budget").over(w_grp).alias("employee_job_title_count_budget"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title_2"),
        F.col("c.level").alias("employee_level"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("budget") >= 4028000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        F.when(F.col("employee_job_title_count_budget") >= 15, "PREMIUM").otherwise("STANDARD").alias("employee_job_title_count_budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.size(F.collect_set("employee_job_title")).alias("num_employee_job_titles"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
    .withColumn("new_york_count_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("budget_tier").desc_nulls_first())
)
