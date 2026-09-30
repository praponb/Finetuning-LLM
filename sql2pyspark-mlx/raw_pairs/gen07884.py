from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id")

cleaned = (
    df_employees
    .filter(F.year("hire_date") >= 2024)
    .select(
        "*",
        F.lower("job_title").alias("job_title_lower"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .filter(F.col("b.location").isin(["New York", "Singapore", "Tokyo"]))
    .select("src.*", F.col("b.dept_name"), F.col("b.budget"))
)

remaining_keys = (
    joined
    .select("job_title")
    .intersect(
        df_employees
        .filter(F.col("salary") > 103000)
        .select("job_title")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

joined_employees = (
    reconciled
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.job_title").alias("employee_job_title"))
)

scored = (
    joined_employees
    .withColumn("manager_id_sum_is_remote_flag", F.sum("is_remote_flag").over(w_grp))
)

result = (
    scored
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.location").alias("location"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.is_remote_flag")).alias("min_is_remote_flag"),
        F.sum(F.col("src.budget")).alias("total_budget"),
        F.grouping_id().alias("level_id"),
    )
)
