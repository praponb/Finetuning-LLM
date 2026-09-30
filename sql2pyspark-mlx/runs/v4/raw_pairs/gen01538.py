from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("location", "job_title").orderBy(F.col("budget").asc_nulls_last())
w5 = Window.partitionBy("job_title", "location").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("dept_name", "job_title").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy(F.col("budget_avg_5").asc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("location").isin(["London", "Berlin", "Singapore"]))
    .withColumn("location_group", F.when(F.col("location").isin(["Singapore", "Chiang Mai"]), "Core").otherwise("Other"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"))
)

with_windows = (
    latest_match
    .select(
        "*",
        F.sum("budget").over(w3).alias("dept_name_sum_budget"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
        F.avg("budget").over(w5).alias("budget_avg_5"),
        F.sum("budget").over(w6).alias("cumulative_budget"),
    )
)

cleaned = (
    with_windows
    .filter(~F.col("dept_name").isin(["Marketing", "HR", "Engineering"]))
    .select(
        "*",
        F.round("budget_avg_5", 0).alias("budget_avg_5_rounded"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") <= 101000), "left_anti")
)

latest = (
    not_flagged
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy("location", "job_title")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.round(F.sum("cumulative_budget") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("cumulative_budget_per_engineer"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.avg("budget_avg_5_rounded").alias("avg_budget_avg_5_rounded"),
    )
    .filter(F.col("cumulative_budget_per_engineer") < 0)
)
