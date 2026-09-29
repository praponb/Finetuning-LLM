from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_closed_at_year_actor = Window.partitionBy(F.year("ticket_closed_at"), "actor").orderBy(F.col("ticket_opened_at").desc(), "id")
w_ticket_channel_by_ticket_clo = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_closed_at").desc())
w_ticket_opened_at_to_date_by = Window.partitionBy(F.to_date("ticket_opened_at")).orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_by_ticket_clo_2 = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_opened_at_to_date_by_2 = Window.partitionBy(F.to_date("ticket_opened_at")).orderBy(F.col("ticket_closed_at").desc(), "id")
w_event_type_ticket_channel_by = Window.partitionBy("event_type", "ticket_channel").orderBy("ticket_opened_at")
w_ticket_id_by_ticket_closed_a = Window.partitionBy("ticket_id").orderBy("ticket_closed_at", "id")

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

ranked = (
    combined_details_ticket
    .select(
        "*",
        F.lag("ticket_opened_at").over(w_ticket_closed_at_year_actor).alias("prev_ticket_opened_at"),
        F.round(F.percent_rank().over(w_ticket_channel_by_ticket_clo), 3).alias("ticket_closed_at_percent_rank"),
        F.nth_value("ticket_opened_at", 2).over(w_ticket_opened_at_to_date_by).alias("second_ticket_opened_at"),
        F.last("ticket_closed_at").over(w_ticket_opened_at_to_date_by).alias("last_ticket_closed_at"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_ticket_channel_by_ticket_clo_2).alias("first_known_ticket_closed_at"),
        F.lag("ticket_closed_at").over(w_ticket_closed_at_year_actor).alias("prev_ticket_closed_at"),
        F.round(F.cume_dist().over(w_ticket_opened_at_to_date_by_2), 3).alias("ticket_closed_at_cume_dist"),
        F.rank().over(w_ticket_closed_at_year_actor).alias("ticket_opened_at_rank"),
        F.rank().over(w_event_type_ticket_channel_by).alias("ticket_opened_at_rank_2"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_ticket_id_by_ticket_closed_a))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
    )
)

first_rows = ranked.filter(F.col("ticket_opened_at_rank_2") == 1)

result = (
    first_rows
    .filter(F.col("prev_ticket_opened_at") < F.lit("2025-03-08 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_opened_at",
        "ticket_closed_at",
        "prev_ticket_opened_at",
        "ticket_closed_at_percent_rank",
        "last_ticket_closed_at",
        "ticket_closed_at_cume_dist",
        "ticket_opened_at_rank",
        "ticket_opened_at_rank_2",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.to_date(F.col("prev_ticket_opened_at")).alias("prev_ticket_opened_at_date"),
    )
    .orderBy(
        F.col("ticket_opened_at_rank").desc_nulls_first(),
        F.col("actor").desc(),
    )
)
