from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_next_event_at_month_seq_no_b = Window.partitionBy(F.date_trunc("month", "next_event_at"), "seq_no").orderBy("event_at_rank", "id")
w_next_event_at_year_event_typ = Window.partitionBy(F.year("next_event_at"), "event_type").orderBy(F.col("next_event_at").desc(), "id")
w_next_event_at_year_event_typ_2 = Window.partitionBy(F.year("next_event_at"), "event_type").orderBy(F.col("next_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type = Window.partitionBy("event_type")

scoped = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label_2"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at"),
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "minutes_since_prev_event_at",
        "prev2_event_at",
        "next_event_at",
        F.round((F.unix_timestamp("prev2_event_at") - F.unix_timestamp(F.lag("prev2_event_at").over(w_next_event_at_month_seq_no_b))) / 60, 1).alias("minutes_since_prev_prev2_event_at"),
        F.ntile(5).over(w_next_event_at_year_event_typ).alias("next_event_at_quintile"),
        F.row_number().over(w_next_event_at_year_event_typ).alias("seq_no_2"),
        F.first("event_at_rank").over(w_next_event_at_year_event_typ_2).alias("first_event_at_rank"),
        F.min("event_at_cume_dist").over(w_event_type).alias("event_type_min_event_at_cume_dist"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "minutes_since_prev_event_at",
        "prev2_event_at",
        "minutes_since_prev_prev2_event_at",
        "next_event_at_quintile",
        "seq_no_2",
        "event_type_min_event_at_cume_dist",
        F.round(F.col("next_event_at_quintile") / F.nullif(F.col("seq_no_2"), F.lit(0)), 3).alias("next_event_at_quintile_per_seq_no_2"),
    )
    .orderBy(F.col("actor").desc_nulls_first())
)
