from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_csat_by_min_requeste = Window.partitionBy("channel", "csat").orderBy("min_requester_id", "closed_at_day", "csat", "channel")
w_first_opened_at_month = Window.partitionBy(F.date_trunc("month", "first_opened_at"))

scoped_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
    )
)

screened = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-12").cast("date")) & (F.col("t.actor").isNotNull()), "left_semi")
)

screened_2 = (
    screened.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 3) & (F.col("t.channel").isNotNull()), "left_semi")
)

rollup_base = (
    screened_2
    .filter(F.col("opened_at_month") > 3)
    .groupBy(F.to_date("closed_at").alias("closed_at_day"), "csat", "channel")
    .agg(
        F.min("requester_id").alias("min_requester_id"),
        F.min("csat").alias("min_csat"),
        F.count("*").alias("cnt"),
        F.sum("csat").alias("total_csat"),
        F.min("opened_at").alias("first_opened_at"),
    )
)

windowed = (
    rollup_base
    .withColumn("prev3_cnt", F.lag("cnt", 3, 0).over(w_channel_csat_by_min_requeste))
    .withColumn("distinct_channel_in_first_opened_at_month", F.size(F.collect_set("channel").over(w_first_opened_at_month)))
)

screened_3 = (
    windowed
    .crossJoin(windowed.agg(F.max("min_requester_id").alias("overall_max_min_requester_id")))
    .filter(F.col("min_requester_id") < F.col("overall_max_min_requester_id"))
    .drop("overall_max_min_requester_id")
)

result = (
    screened_3
    .select(
        "closed_at_day",
        "channel",
        "min_requester_id",
        "min_csat",
        "total_csat",
        "prev3_cnt",
        "distinct_channel_in_first_opened_at_month",
        F.abs(F.col("min_csat") - 4).alias("min_csat_distance"),
        F.coalesce(F.col("min_requester_id"), F.lit(0)).alias("min_requester_id_filled"),
    )
    .orderBy(F.col("channel").asc_nulls_last())
)
