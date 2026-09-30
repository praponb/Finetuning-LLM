from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_year = Window.partitionBy("event_type").orderBy("event_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at_year_2 = Window.partitionBy("event_type").orderBy("event_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_year_by_event_at_ye = Window.partitionBy("event_at_year").orderBy(F.col("event_at_year").desc_nulls_first(), "id")
w_latest = Window.partitionBy("ticket_id").orderBy(F.col("ticket_closed_at").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("ticket_channel", "ticket_agent_id").orderBy(F.col("last_known_event_at_year").desc(), F.col("id").desc())

scoped = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) | (~F.col("actor").isin("customer", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.year(F.col("event_at")).alias("event_at_year"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_prefix",
        "actor_event_type_label",
        F.last("event_at_year", ignorenulls=True).over(w_event_type_by_event_at_year).alias("last_known_event_at_year"),
        F.last("event_at").over(w_event_type_by_event_at_year_2).alias("last_event_at"),
        F.round(F.col("event_at_year") - F.avg("event_at_year").over(w_ticket_id), 2).alias("event_at_year_vs_ticket_id_avg"),
        F.lag("event_at_year", 2, 0).over(w_event_at_year_by_event_at_ye).alias("prev2_event_at_year"),
        F.min("event_at_year").over(w_ticket_id).alias("ticket_id_min_event_at_year"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("last_known_event_at_year").cast("string")).alias("actor_prefix_ref"),
        F.date_trunc("year", F.col("last_event_at")).alias("last_event_at_year"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_details = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor_prefix",
        "s.actor_event_type_label",
        "s.last_known_event_at_year",
        "s.prev2_event_at_year",
        "s.ticket_id_min_event_at_year",
        "s.actor_prefix_ref",
        "s.event_count",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

latest = (
    with_details
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_2 = (
    latest
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_2
    .filter(F.col("actor_prefix").isNotNull())
    .select(
        "id",
        "ticket_id",
        "actor_prefix",
        "prev2_event_at_year",
        "ticket_id_min_event_at_year",
        "actor_prefix_ref",
        "event_count",
        "ticket_agent_id",
        "ticket_closed_at",
        F.concat(F.col("actor_event_type_label"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_event_type_label_ref"),
    )
)
