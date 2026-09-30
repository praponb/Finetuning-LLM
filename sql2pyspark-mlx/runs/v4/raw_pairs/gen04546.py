from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy("budget")
w3 = Window.orderBy("budget")
w4 = Window.partitionBy("employee_job_title")
w5 = Window.partitionBy("dept_name", "location")
w6 = Window.orderBy(F.col("budget").desc_nulls_first())
w7 = Window.orderBy(F.col("budget_share").desc_nulls_last())

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.level").isin(["L4", "L1"])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.level").alias("employee_level"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

kept_keys = (
    deduped
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Singapore", "New York"]))
        .select("id", "dept_name")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("budget_percent_rank", F.percent_rank().over(w3))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
    .withColumn("budget_share_2", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
    .withColumn("budget_share_3", F.round((F.col("budget") / F.sum("budget").over(w5)), 4))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") <= "2025-10-01"), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.job_title").isin(["Engineer", "Intern"])), "left_semi")
)

result = (
    kept_departments
    .withColumn("budget_rank", F.rank().over(w6))
    .withColumn("budget_share_percent_rank", F.percent_rank().over(w7))
)
