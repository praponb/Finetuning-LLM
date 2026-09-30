from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").desc(), "id")
w4 = Window.partitionBy("page").orderBy(F.col("event_time_period").desc_nulls_first(), "id")
w5 = Window.partitionBy(F.year("event_time"), "session_id").orderBy("running_duration_sec", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.year("event_time_period")).orderBy("event_time_period", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

valid = (
    df_web_events
    .filter(F.col("duration_sec") >= 288)
    .withColumn("event_time_period", F.date_trunc("week", "event_time"))
    .withColumn("page_filled", F.coalesce("page", F.lit("UNKNOWN")))
)

enriched = (
    valid
    .filter(F.col("event_type") == "page_view")
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked_web_events = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "session_id",
        "event_time",
        "event_time_period",
        "last_known_page",
        "running_duration_sec",
    )
)

enriched_web_events = (
    ranked_web_events
    .withColumn("running_duration_sec_avg_6_prior", F.avg("running_duration_sec").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

totals = (
    enriched_web_events
    .cube("session_id")
    .agg(
        F.min("running_duration_sec_avg_6_prior").alias("min_running_duration_sec_avg_6_prior"),
        F.min("running_duration_sec").alias("min_running_duration_sec"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.percentile_approx("running_duration_sec", [0.25, 0.5, 0.75]).alias("running_duration_sec_quartiles"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    totals
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "min_running_duration_sec",
        "distinct_session_ids",
        "running_duration_sec_quartiles",
        "num_rows",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
