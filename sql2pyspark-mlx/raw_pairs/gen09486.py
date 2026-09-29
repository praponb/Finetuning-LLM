from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title").orderBy("budget", "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy("job_title")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.job_title").isin(["Designer", "Manager", "Director"])), "left_anti")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L2", "L1", "L5"])), "left_semi")
)

with_latest = (
    kept_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", F.col("c.job_title"))
)

valid = (
    with_latest
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.upper("job_title").alias("job_title_upper"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

result = (
    valid
    .filter(F.col("job_title") != "Manager")
    .select(
        "id",
        "budget",
        "job_title",
        F.ntile(3).over(w2).alias("budget_bucket"),
        F.lead("budget", 1).over(w2).alias("next_budget"),
        F.percent_rank().over(w3).alias("budget_percent_rank"),
        F.count("budget").over(w4).alias("job_title_count_budget"),
    )
)
