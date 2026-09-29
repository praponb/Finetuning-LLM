from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("browser")
w5 = Window.orderBy("browser_avg_session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_min_session_id_times_browser_avg_session_id", "id").rowsBetween(-2, Window.currentRow)

with_dims = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select("s.*", F.col("c.signup_date"))
)

prev_events = with_dims.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "customer_id",
        "started_at",
        F.min("session_id").over(w_ordered).alias("running_min_session_id"),
        F.count("session_id").over(w_ordered).alias("running_count_session_id"),
        F.avg("session_id").over(w_ordered).alias("running_avg_session_id"),
        F.sum("session_id").over(w_ordered).alias("running_sum_session_id"),
        F.avg("session_id").over(w_recent).alias("browser_avg_session_id"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

filtered = (
    kept
    .filter(F.col("started_at") > F.lit("2023-08-18").cast("date"))
    .select(
        "id",
        "started_at",
        "running_min_session_id",
        "running_sum_session_id",
        "browser_avg_session_id",
        F.coalesce("running_min_session_id", F.lit(0)).alias("running_min_session_id_filled"),
        (F.col("running_min_session_id") * F.col("browser_avg_session_id")).alias("running_min_session_id_times_browser_avg_session_id"),
    )
)

metrics_sessions = (
    filtered
    .withColumn("running_avg_running_sum_session_id", F.avg("running_sum_session_id").over(w5))
    .withColumn("running_min_running_sum_session_id", F.min("running_sum_session_id").over(w5))
    .withColumn("running_max_running_sum_session_id", F.max("running_sum_session_id").over(w5))
    .withColumn("running_min_session_id_filled_avg_2", F.avg("running_min_session_id_filled").over(w6))
)

result = (
    metrics_sessions
    .select(
        "id",
        "running_min_session_id_times_browser_avg_session_id",
        "running_max_running_sum_session_id",
        "running_min_running_sum_session_id",
    )
    .unpivot(
        ids=["id"],
        values=["running_min_session_id_times_browser_avg_session_id", "running_max_running_sum_session_id", "running_min_running_sum_session_id"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
