from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_id", "job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-09-02").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
        F.coalesce("level", F.lit("NONE")).alias("level_clean"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.email"))
)

with_windows = (
    with_dims
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w2))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w3))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)

result = (
    enriched
    .groupBy("manager_id", F.date_trunc("YEAR", "hire_date").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("level") == "L1", F.col("event_seq"))).alias("l1_avg_event_seq"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum("event_seq").alias("total_event_seq"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
