from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title").orderBy(F.col("budget").asc_nulls_last(), "id")

cleaned = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "HR", "Legal"]))
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Engineering", "Sales"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.job_title"))
)

latest = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("dept_name", "location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
    .orderBy(F.col("location").desc())
)
