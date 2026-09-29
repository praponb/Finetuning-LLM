from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w3 = Window.orderBy("location_length", "id")
w_grp = Window.partitionBy("dept_name", "location")
w5 = Window.partitionBy("dept_name")

filtered = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select("*", F.length("location").alias("location_length"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level").isin(["L5", "L4"])), "left_anti")
)

scored = (
    eligible
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("budget_cume_dist", F.cume_dist().over(w2))
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("K%")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.lag("event_seq", 1).over(w3).alias("prev_event_seq"),
        F.min("event_seq").over(w_grp).alias("dept_name_min_event_seq"),
        F.count("budget").over(w5).alias("dept_name_count_budget"),
    )
)

grouped = (
    metrics
    .groupBy(
        "location",
        F.when(F.col("prev_event_seq") >= 8, "HIGH").otherwise("LOW").alias("prev_event_seq_tier"),
    )
    .agg(
        F.percentile_approx("budget_cume_dist", [0.25, 0.5, 0.75]).alias("budget_cume_dist_quartiles"),
        F.avg(F.when(F.col("location") == "Chiang Mai", F.col("prev_event_seq"))).alias("chiang_mai_avg_prev_event_seq"),
        F.avg(F.when(F.col("location") == "Tokyo", F.col("event_seq"))).alias("tokyo_avg_event_seq"),
    )
    .filter(F.col("tokyo_avg_event_seq") > 0)
)

result = (
    grouped
    .select(
        "location",
        "prev_event_seq_tier",
        "budget_cume_dist_quartiles",
        "chiang_mai_avg_prev_event_seq",
        "tokyo_avg_event_seq",
    )
    .orderBy("location")
)
