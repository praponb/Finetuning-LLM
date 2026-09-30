from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_windows = (
    df_departments
    .select(
        "*",
        F.ntile(5).over(w).alias("budget_bucket"),
        F.percent_rank().over(w2).alias("budget_percent_rank"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.hire_date"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.hire_date",
        F.col("c.job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

matched_keys = (
    with_latest
    .select("id")
    .exceptAll(
        df_departments
        .filter(F.col("budget") >= 2789000)
        .select("id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("job_title").isNotNull())
    .select("*", F.initcap(F.trim("job_title")).alias("job_title_title"))
)

result = (
    filtered
    .select("id", "hire_date", "job_title", "employee_salary", "job_title_title")
    .orderBy("employee_salary")
)
