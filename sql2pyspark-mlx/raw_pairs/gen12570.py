from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.orderBy(F.col("budget").desc(), "id")
w_hist = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy("dept_name_avg_budget", "id")

enriched = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.avg("budget").over(w).alias("dept_name_avg_budget"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched_departments = (
    enriched
    .select(
        "id",
        "dept_name",
        "event_seq",
        F.lag("dept_name_avg_budget").over(w_hist).alias("prev_dept_name_avg_budget"),
        F.lag("budget").over(w_hist).alias("prev_budget"),
        F.row_number().over(w_hist).alias("event_seq_2"),
        F.row_number().over(w4).alias("event_seq_3"),
    )
)

remaining_keys = (
    enriched_departments
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("location").isin(["Singapore", "Tokyo"]))
        .select("id")
    )
)

kept_rows = (
    enriched_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .select(
        "id",
        "event_seq",
        "prev_dept_name_avg_budget",
        "prev_budget",
        "event_seq_2",
        "event_seq_3",
        F.round((F.col("prev_budget") * 0.85), 2).alias("prev_budget_adjusted"),
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
    )
)

result = (
    valid
    .withColumn("prev_dept_name_avg_budget_tier", F.when(F.col("prev_dept_name_avg_budget") >= 4502613, "HIGH").otherwise("LOW"))
    .withColumn("is_hr_tier", F.when(F.col("is_hr") >= 81, "HIGH").otherwise("LOW"))
    .rollup("prev_dept_name_avg_budget_tier", "is_hr_tier")
    .agg(
        F.percentile_approx("is_hr", [0.25, 0.5, 0.75]).alias("is_hr_quartiles"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.avg("prev_budget_adjusted").alias("avg_prev_budget_adjusted"),
        F.round(F.avg("is_hr"), 2).alias("avg_is_hr"),
        F.round(F.avg("prev_dept_name_avg_budget"), 2).alias("avg_prev_dept_name_avg_budget"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("event_seq_p90").desc())
)
