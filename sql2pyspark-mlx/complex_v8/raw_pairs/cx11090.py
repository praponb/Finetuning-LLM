from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_hour_full = Window.orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_agent_id_event_at_to = Window.partitionBy("ticket_agent_id", F.to_date("event_at")).orderBy(F.col("event_at_hour").desc(), "id")
w_event_type_by_event_at_hour = Window.partitionBy("event_type").orderBy("event_at_hour", "id")
w_event_type_by_event_at_hour_2 = Window.partitionBy("event_type").orderBy("event_at_hour")
w_latest = Window.partitionBy("event_type").orderBy(F.col("second_event_at_hour").desc(), F.col("id").desc())

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.col("ticket_id") >= 48))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

with_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select("s.*", F.col("t.agent_id").alias("ticket_agent_id"))
)

windowed = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_hour",
        F.nth_value("event_at_hour", 2).over(w_all_by_event_at_hour_full).alias("second_event_at_hour"),
        F.lag("event_at").over(w_ticket_agent_id_event_at_to).alias("prev_event_at"),
        F.lag("event_at").over(w_event_type_by_event_at_hour).alias("prev_event_at_2"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_hour_2), 3).alias("event_at_hour_cume_dist"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.second_event_at_hour",
        "s.prev_event_at_2",
        "s.event_at_hour_cume_dist",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

latest_per_key = (
    joined
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

filtered = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.max("event_at_hour_cume_dist").alias("overall_max_event_at_hour_cume_dist")))
    .filter(F.col("event_at_hour_cume_dist") < F.col("overall_max_event_at_hour_cume_dist"))
    .drop("overall_max_event_at_hour_cume_dist")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "second_event_at_hour",
        "prev_event_at_2",
        "event_at_hour_cume_dist",
        "ticket_opened_at",
        "ticket_tags",
    )
)
