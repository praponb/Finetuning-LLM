from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser", "device").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.partitionBy("customer_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("running_avg_event_seq_2", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("customer_id").orderBy("running_avg_event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
)

enriched_sessions = (
    enriched
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

cleaned = (
    enriched_sessions
    .filter(F.col("country").eqNullSafe(F.lit("US")))
    .select(
        "*",
        F.when(F.col("country").isin(["TH", "UK"]), "Core").otherwise("Other").alias("country_group"),
    )
)

with_windows = (
    cleaned
    .withColumn("running_avg_event_seq_2", F.avg("event_seq_2").over(w_recent))
    .withColumn("running_max_event_seq_2", F.max("event_seq_2").over(w_recent))
    .withColumn("running_sum_event_seq_2", F.sum("event_seq_2").over(w_recent))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.country",
        "e.event_seq",
        "e.event_seq_2",
        "e.running_avg_event_seq_2",
        F.col("c.age"),
    )
)

enriched_sessions_2 = (
    with_dims
    .withColumn("cumulative_age", F.sum("age").over(w5))
    .withColumn("event_seq_2_avg_2_prior", F.avg("event_seq_2").over(w6))
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w7))
)

result = (
    enriched_sessions_2
    .select(
        "id",
        "browser",
        "event_seq_2",
        "running_avg_event_seq_2",
        "event_seq_2_avg_2_prior",
    )
    .orderBy(F.col("running_avg_event_seq_2").asc_nulls_last(), "event_seq_2")
)
