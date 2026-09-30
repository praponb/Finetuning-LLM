from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("budget_rounded", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("location").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("budget_avg_6_prior").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w5 = Window.partitionBy("location").orderBy("budget_avg_6_prior", "id")
w6 = Window.partitionBy("dept_name").orderBy("budget_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("budget").between(1630000, 3942000))
    .select(
        "*",
        F.round("budget", 1).alias("budget_rounded"),
        F.upper("location").alias("location_upper"),
    )
)

scored = (
    cleaned
    .withColumn("budget_avg_6_prior", F.avg("budget").over(w_part))
    .withColumn("running_budget_rounded", F.sum("budget_rounded").over(w2))
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNotNull()), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.percent_rank().over(w4).alias("budget_percent_rank"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.sum("running_budget_rounded").over(w6).alias("running_running_budget_rounded"),
    )
)

result = (
    enriched
    .withColumn("budget_avg_6_prior_tier", F.when(F.col("budget_avg_6_prior") >= 3565267, "LARGE").otherwise("SMALL"))
    .cube("budget_avg_6_prior_tier", "dept_name")
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.round(F.sum("row_seq_2") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("row_seq_2_per_london"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
