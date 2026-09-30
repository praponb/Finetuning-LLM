from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w4 = Window.orderBy("budget")

latest = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_latest = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.salary"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.dense_rank().over(w3).alias("budget_dense_rank"),
        F.percent_rank().over(w4).alias("budget_percent_rank"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Director"), "left_semi")
)

daily = (
    not_flagged
    .groupBy("employee_level")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    daily
    .select(
        "employee_level",
        "distinct_ids",
        "event_count",
        "budget_stddev",
        "num_dept_names",
    )
    .orderBy(F.col("employee_level").desc_nulls_first())
    .limit(20)
)
