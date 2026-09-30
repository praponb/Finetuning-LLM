from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy("dept_name_count_budget", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget", "id")
w_ordered = Window.partitionBy("location").orderBy(F.col("budget_avg_2_prior").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & ~F.col("location").isin(["Chiang Mai", "Berlin"]))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("resolved_location", F.coalesce("location", F.lit("DIRECT")))
)

scored = (
    valid
    .filter(F.col("budget") < 4018000)
    .withColumn("last_known_budget", F.last("budget", ignorenulls=True).over(w_running))
    .withColumn("budget_avg_2_prior", F.avg("budget").over(w2))
    .withColumn("dept_name_count_budget", F.count("budget").over(w3))
)

with_windows = (
    scored
    .select(
        "id",
        "dept_name",
        "location",
        "resolved_location",
        "last_known_budget",
        "budget_avg_2_prior",
        "dept_name_count_budget",
        F.avg("dept_name_count_budget").over(w4).alias("dept_name_count_budget_avg_3"),
        F.lead("dept_name_count_budget", 2).over(w5).alias("next_dept_name_count_budget"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.resolved_location",
        "s.dept_name_count_budget",
        "s.next_dept_name_count_budget",
        "s.event_seq",
        F.col("c.level"),
        F.col("c.job_title"),
    )
)

result = (
    with_latest
    .withColumn("next_dept_name_count_budget_tier", F.when(F.col("next_dept_name_count_budget") >= 16, "PREMIUM").otherwise("STANDARD"))
    .cube("next_dept_name_count_budget_tier")
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.avg("dept_name_count_budget").alias("avg_dept_name_count_budget"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("next_dept_name_count_budget_tier").asc_nulls_first())
)
