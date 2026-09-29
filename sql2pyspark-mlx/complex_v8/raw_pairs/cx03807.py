from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").desc(), "id")
w_ticket_channel_by_event_at_d = Window.partitionBy("ticket_channel").orderBy("event_at_date", "id")
w_event_type_by_ticket_opened = Window.partitionBy("event_type").orderBy("ticket_opened_at", "id")
w_all_by_ticket_opened_at_ties = Window.orderBy(F.col("ticket_opened_at").desc())
w_event_type_by_ticket_opened_2 = Window.partitionBy("event_type").orderBy("ticket_opened_at")
w_prev2_ticket_opened_at_month = Window.partitionBy(F.month("prev2_ticket_opened_at"), "ticket_opened_at_quartile")
w_seq_no_by_ticket_opened_at_r = Window.partitionBy("seq_no").orderBy("ticket_opened_at_rank")

filtered = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.hour(F.col("event_at")).between(8, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

with_details_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel") != "email")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_date",
        "s.actor_ref",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

with_windows = (
    with_details_ticket
    .select(
        "id",
        "event_type",
        "actor",
        "actor_ref",
        "ticket_opened_at",
        "ticket_channel",
        F.round(F.cume_dist().over(w_all_by_ticket_opened_at), 3).alias("ticket_opened_at_cume_dist"),
        F.row_number().over(w_ticket_channel_by_event_at_d).alias("seq_no"),
        F.ntile(5).over(w_all_by_ticket_opened_at).alias("ticket_opened_at_quintile"),
        F.dense_rank().over(w_all_by_ticket_opened_at).alias("ticket_opened_at_dense_rank"),
        F.lead("event_at_date").over(w_event_type_by_ticket_opened).alias("next_event_at_date"),
        F.lag("ticket_opened_at", 2).over(w_all_by_ticket_opened_at).alias("prev2_ticket_opened_at"),
        F.row_number().over(w_all_by_ticket_opened_at).alias("seq_no_2"),
        F.dense_rank().over(w_event_type_by_ticket_opened).alias("ticket_opened_at_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_ticket_opened_at_ties), 3).alias("ticket_opened_at_percent_rank"),
        F.ntile(4).over(w_all_by_ticket_opened_at).alias("ticket_opened_at_quartile"),
        F.round(F.percent_rank().over(w_ticket_channel_by_event_at_d), 3).alias("event_at_date_percent_rank"),
        F.rank().over(w_event_type_by_ticket_opened_2).alias("ticket_opened_at_rank"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.max("ticket_opened_at_dense_rank").over(w_prev2_ticket_opened_at_month).alias("prev2_ticket_opened_at_month_ticket_opened_at_qu"),
        F.rank().over(w_seq_no_by_ticket_opened_at_r).alias("ticket_opened_at_rank_rank"),
        F.dense_rank().over(w_seq_no_by_ticket_opened_at_r).alias("ticket_opened_at_rank_dense_rank"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "event_type",
        "actor",
        "actor_ref",
        "ticket_opened_at",
        "ticket_channel",
        "ticket_opened_at_cume_dist",
        "ticket_opened_at_dense_rank",
        "next_event_at_date",
        "prev2_ticket_opened_at",
        "seq_no_2",
        "ticket_opened_at_dense_rank_2",
        "ticket_opened_at_quartile",
        "event_at_date_percent_rank",
        "ticket_opened_at_rank",
        "ticket_opened_at_rank_rank",
        "ticket_opened_at_rank_dense_rank",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.least(F.col("ticket_opened_at_rank"), F.col("seq_no_2")).alias("least_ticket_opened_at_rank_seq_no_2"),
    )
    .orderBy("ticket_opened_at_dense_rank_2")
)
