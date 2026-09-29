from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w_grp = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

scored = (
    df_departments
    .select(
        "id",
        "budget",
        F.sum("budget").over(w_recent).alias("running_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.avg("budget").over(w_grp).alias("budget_avg_5_prior"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share_2"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

base = (
    latest_match
    .filter(F.col("budget_avg_5_prior").isNotNull())
    .withColumn("budget_share_rounded", F.round("budget_share", 2))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id").isin([11, 24, 36, 39])), "left_anti")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.col("c.full_name")).like("%avi%")), "left_semi")
)

totals = (
    eligible_departments
    .groupBy(
        F.when(F.col("budget") >= 3225000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("budget_share", [0.25, 0.5, 0.75]).alias("budget_share_quartiles"),
        F.percentile_approx("budget_share_2", 0.5).alias("budget_share_2_p50"),
    )
    .filter(F.col("budget_share_2_p50") > 0)
)

result = (
    totals
    .select(
        "budget_tier",
        "quarter_start",
        "budget_share_quartiles",
        "budget_share_2_p50",
    )
)
