from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_channel_actor_by_even = Window.partitionBy("ticket_channel", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_channel_actor_by_even_2 = Window.partitionBy("ticket_channel", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_actor_by_even_3 = Window.partitionBy("ticket_channel", "actor").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_channel_by_event_at_f = Window.partitionBy("ticket_channel").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_by_event_at_r = Window.partitionBy("ticket_channel").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_channel_by_event_at_t = Window.partitionBy("ticket_channel").orderBy("event_at")

base = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.channel").alias("ticket_channel"),
    )
)

with_trends = (
    with_parent_ticket
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_channel_actor_by_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_ticket_channel_actor_by_even_2).alias("second_event_at"),
        F.rank().over(w_ticket_channel_actor_by_even_3).alias("event_at_rank"),
        F.lag("event_at", 2).over(w_ticket_channel_actor_by_even).alias("prev2_event_at"),
        F.ntile(5).over(w_ticket_id_by_event_at).alias("event_at_quintile"),
        F.first("event_at").over(w_ticket_channel_by_event_at_f).alias("first_event_at"),
        F.first("event_at", ignorenulls=True).over(w_ticket_channel_by_event_at_r).alias("first_known_event_at_3"),
        F.ntile(10).over(w_event_type_by_event_at).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_ticket_channel_actor_by_even), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.dense_rank().over(w_ticket_channel_by_event_at_t).alias("event_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "first_known_event_at",
        "event_at_seq",
        "event_at_quintile",
        "first_event_at",
        "first_known_event_at_3",
        "event_at_decile",
        "event_at_percent_rank",
        "next_event_at",
        "event_at_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_channel")), " ", "_")).alias("ticket_channel_slug"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)
