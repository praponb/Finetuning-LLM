from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget_int").desc(), "id")
w3 = Window.orderBy("budget_int", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("running_budget_int", "id")
w5 = Window.orderBy("running_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.when(F.col("location").isin(["Bangkok", "Tokyo"]), "Core").otherwise("Other").alias("location_group"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.col("c.email")).like("%vy@%")), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("location") != "New York")
    .select(
        "id",
        "budget",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("resolved_dept_name"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("budget_int").over(w_grp).alias("running_budget_int"),
        F.row_number().over(w2).alias("seq_no"),
        F.sum("budget").over(w_grp).alias("running_budget"),
        F.avg("budget").over(w3).alias("budget_avg_2_prior"),
    )
)

metrics = (
    with_windows
    .withColumn("next_seq_no", F.lead("seq_no").over(w4))
    .withColumn("prev_running_budget_int", F.lag("running_budget_int").over(w4))
    .withColumn("next_running_budget_int", F.lead("running_budget_int").over(w4))
    .withColumn("seq_no_2", F.row_number().over(w4))
    .withColumn("cumulative_budget_int", F.sum("budget_int").over(w5))
)

result = (
    metrics
    .groupBy("seq_no", "next_seq_no")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.percentile_approx("running_budget_int", [0.25, 0.5, 0.75]).alias("running_budget_int_quartiles"),
        F.percentile_approx("seq_no_2", 0.95).alias("seq_no_2_p95"),
        F.percentile_approx("cumulative_budget_int", 0.5).alias("cumulative_budget_int_p50"),
    )
    .filter(F.col("seq_no_2_p95") < 1000000)
)
