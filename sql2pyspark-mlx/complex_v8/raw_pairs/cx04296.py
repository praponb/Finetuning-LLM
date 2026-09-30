from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_closed_at_ties = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc())
w_csat = Window.partitionBy("csat")
w_csat_by_opened_at = Window.partitionBy("csat").orderBy("opened_at", "id")
w_first_event_at_month_by_tota = Window.partitionBy(F.date_trunc("month", "first_event_at")).orderBy("total_closed_at_rank", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_event_at_month_by_tota_2 = Window.partitionBy(F.date_trunc("month", "first_event_at")).orderBy("total_closed_at_rank", "event_id")
w_last_event_at_year_event_id = Window.partitionBy(F.year("last_event_at"), "event_id")

valid_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 6) | (F.col("requester_id") > 7))
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
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

windowed = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "channel_ref",
        "tags_text",
        F.rank().over(w_agent_id_by_closed_at_ties).alias("closed_at_rank"),
        F.min("closed_at_dayofweek").over(w_csat).alias("csat_min_closed_at_dayofweek"),
        (F.col("closed_at_dayofweek") - F.lag("closed_at_dayofweek").over(w_csat_by_opened_at)).alias("closed_at_dayofweek_change"),
    )
)

exploded_detail_events = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.channel_ref",
        "s.tags_text",
        "s.closed_at_rank",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

stats = (
    exploded_detail_events
    .groupBy("event_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.sum("closed_at_rank").alias("total_closed_at_rank"),
        F.max("event_at").alias("last_event_at"),
        F.sum("csat").alias("sum_csat_check"),
    )
    .filter(F.col("sum_csat_check") > 2)
    .drop("sum_csat_check")
)

metrics = (
    stats
    .select(
        "*",
        F.nth_value("last_event_at", 2).over(w_first_event_at_month_by_tota).alias("second_last_event_at"),
        F.ntile(5).over(w_first_event_at_month_by_tota_2).alias("total_closed_at_rank_quintile"),
        F.round(F.col("total_closed_at_rank") / F.nullif(F.sum("total_closed_at_rank").over(w_last_event_at_year_event_id), F.lit(0)), 4).alias("total_closed_at_rank_share_of_last_event_at_year"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.month(F.col("last_event_at")).alias("last_event_at_month"),
        F.date_trunc("month", F.col("first_event_at")).alias("first_event_at_month"),
    )
)

result = (
    annotated
    .filter(F.col("last_event_at_month") <= 10)
    .select(
        "event_id",
        "first_event_at",
        "last_event_at",
        "total_closed_at_rank_quintile",
        "total_closed_at_rank_share_of_last_event_at_year",
        "last_event_at_month",
        "first_event_at_month",
        F.date_trunc("hour", F.col("second_last_event_at")).alias("second_last_event_at_hour"),
        F.abs(F.col("last_event_at_month") - 6).alias("last_event_at_month_distance"),
    )
    .orderBy(F.col("last_event_at_month_distance").asc_nulls_last())
)
