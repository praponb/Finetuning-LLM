from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("dept_name", "job_title")
w4 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("budget_int", "id").rowsBetween(-5, -1)
w6 = Window.orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.col("budget").cast("int").alias("budget_int"))
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

metrics = (
    latest_match
    .filter(F.col("budget_int") == 21)
    .withColumn("budget_int_avg_5", F.avg("budget_int").over(w2))
    .withColumn("dept_name_sum_budget", F.sum("budget").over(w3))
    .withColumn("running_budget", F.sum("budget").over(w4))
    .withColumn("budget_int_avg_5_prior", F.avg("budget_int").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    metrics
    .rollup(
        "location",
        F.when(F.col("budget") >= 4343000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.round(F.avg("budget_int"), 2).alias("avg_budget_int"),
        F.percentile_approx("budget_int", [0.25, 0.5, 0.75]).alias("budget_int_quartiles"),
    )
    .filter(F.col("legal_events") >= 1)
    .orderBy(F.col("budget_tier").desc_nulls_first())
)
