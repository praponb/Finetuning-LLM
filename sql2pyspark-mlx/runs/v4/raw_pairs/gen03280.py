from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("seq_no", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")
w5 = Window.partitionBy("device").orderBy(F.col("seq_no_avg_5_prior_rounded").asc_nulls_last())
w_recent = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device").orderBy(F.col("event_seq").desc_nulls_last())
w9 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w10 = Window.partitionBy("country", "browser").orderBy(F.col("event_seq").desc_nulls_last())
w_running = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_sessions
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w_grp).alias("first_known_country"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows_sessions = (
    with_windows
    .select(
        "*",
        F.avg("seq_no").over(w3).alias("seq_no_avg_5_prior"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

cleaned = (
    with_windows_sessions
    .filter(~F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "first_known_country",
        "event_seq",
        F.round("seq_no_avg_5_prior", 1).alias("seq_no_avg_5_prior_rounded"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= "2023-10-01"), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.last("seq_no_avg_5_prior_rounded", ignorenulls=True).over(w_recent).alias("last_known_seq_no_avg_5_prior_rounded"),
        F.sum("event_seq").over(w_hist).alias("cumulative_event_seq"),
        F.cume_dist().over(w8).alias("event_seq_cume_dist"),
    )
)

with_windows_sessions_2 = (
    metrics
    .withColumn("last_known_seq_no_avg_5_prior_rounded_count_7d", F.count("last_known_seq_no_avg_5_prior_rounded").over(w9))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w10))
    .withColumn("event_seq_cume_dist_count_90d", F.count("event_seq_cume_dist").over(w_running))
)

result = (
    with_windows_sessions_2
    .rollup(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        "device",
        F.when(F.col("event_seq_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_dense_rank_tier"),
    )
    .agg(
        F.min("started_at").alias("first_seen"),
        F.percentile_approx("last_known_seq_no_avg_5_prior_rounded_count_7d", 0.9).alias("last_known_seq_no_avg_5_prior_rounded_count_7d_p90"),
        F.max("started_at").alias("last_started_at"),
    )
    .filter(F.col("last_known_seq_no_avg_5_prior_rounded_count_7d_p90") < 0)
)
