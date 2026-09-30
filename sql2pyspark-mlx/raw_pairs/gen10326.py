from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("budget", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", F.col("c.hire_date"), F.col("c.level"))
)

scored = (
    latest_match
    .select(
        "id",
        "budget",
        "level",
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.last("budget", ignorenulls=True).over(w3).alias("last_known_budget"),
        F.lag("budget", 1).over(w4).alias("prev_budget"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.level", "s.budget_share", F.col("c.hire_date"))
)

totals = (
    with_latest
    .groupBy(
        F.when(F.col("budget_share") >= 0.75, "LARGE").otherwise("SMALL").alias("budget_share_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("budget_share", 0.95).alias("budget_share_p95"),
        F.count_if(F.col("level") == "L3").alias("l3_events"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.avg("budget_share").alias("avg_budget_share"),
        F.round(F.sum("budget_share") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("budget_share_per_l1"),
    )
)

result = (
    totals
    .select("budget_share_tier", "budget_share_p95", "l3_events", "avg_budget_share")
)
