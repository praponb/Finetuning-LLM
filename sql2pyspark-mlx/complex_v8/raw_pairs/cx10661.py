from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_session_start_year_by = Window.partitionBy("events", F.year("session_start")).orderBy(F.col("session_requester_id").asc_nulls_last())
w_channel_session_start_to_dat = Window.partitionBy("channel", F.to_date("session_start"))
w_burst_id_channel = Window.partitionBy("burst_id", "channel")
w_all_by_rows_in_channel_sessi = Window.orderBy("rows_in_channel_session_start_to_date", "channel", "burst_id").rowsBetween(-1, -1)
w_all_by_rows_in_channel_sessi_2 = Window.orderBy("rows_in_channel_session_start_to_date", "channel", "burst_id")

filtered = (
    df_tickets
    .filter(F.col("csat") <= 5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

with_prev = filtered.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_channel_running))

session_summary = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    session_summary
    .select(
        "channel",
        "burst_id",
        F.round(F.percent_rank().over(w_events_session_start_year_by), 3).alias("session_requester_id_percent_rank"),
        F.count(F.lit(1)).over(w_channel_session_start_to_dat).alias("rows_in_channel_session_start_to_date"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.min("burst_id").over(w_burst_id_channel).alias("burst_id_channel_min_burst_id"),
        F.round(F.avg("burst_id").over(w_all_by_rows_in_channel_sessi), 2).alias("burst_id_avg_1_prior"),
        F.count(F.lit(1)).over(w_burst_id_channel).alias("rows_in_burst_id_channel"),
        F.row_number().over(w_all_by_rows_in_channel_sessi_2).alias("seq_no"),
    )
)

result = (
    windowed
    .select(
        "channel",
        "burst_id",
        "burst_id_channel_min_burst_id",
        "rows_in_burst_id_channel",
        "rows_in_channel_session_start_to_date",
    )
    .unpivot(
        ids=["channel", "burst_id"],
        values=["burst_id_channel_min_burst_id", "rows_in_burst_id_channel", "rows_in_channel_session_start_to_date"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("channel", "burst_id", "metric_name")
)
