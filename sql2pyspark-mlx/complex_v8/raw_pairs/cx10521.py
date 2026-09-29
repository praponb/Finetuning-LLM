from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_hour = Window.partitionBy("ticket_id").orderBy(F.col("event_at_hour").desc(), "id")
w_ticket_id_by_event_at_hour_r = Window.partitionBy("ticket_id").orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_hour_f = Window.partitionBy("ticket_id").orderBy("event_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_hour_t = Window.partitionBy("ticket_id").orderBy("event_at_hour")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_hour_t_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at_hour").desc())
w_ticket_id_by_event_at_hour_r_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_hour_2 = Window.partitionBy("ticket_id").orderBy("event_at_hour", "id")
w_event_at_hour_decile_seq_no = Window.partitionBy("event_at_hour_decile", "seq_no")
w_next3_event_at_month_event_t = Window.partitionBy(F.date_trunc("month", "next3_event_at"), "event_type").orderBy("prev2_event_at", "id")
w_all_by_event_at_hour_rank_fu = Window.orderBy("event_at_hour_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type") != "assigned") & (F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

ranked = (
    prepared_events
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_hour",
        "event_type_ref",
        F.lag("event_at", 2).over(w_event_type_ticket_id_by_even).alias("prev2_event_at"),
        F.rank().over(w_ticket_id_by_event_at_hour).alias("event_at_hour_rank"),
        F.last("event_at_hour", ignorenulls=True).over(w_ticket_id_by_event_at_hour_r).alias("last_known_event_at_hour"),
        F.row_number().over(w_event_type_ticket_id_by_even).alias("seq_no"),
        F.last("event_at_hour").over(w_ticket_id_by_event_at_hour_f).alias("last_event_at_hour"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_hour_t), 3).alias("event_at_hour_percent_rank"),
        F.last("event_at_hour").over(w_event_type_ticket_id_by_even_2).alias("last_event_at_hour_2"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_hour_t_2), 3).alias("event_at_hour_cume_dist"),
        F.round((F.unix_timestamp("event_at_hour") - F.unix_timestamp(F.lag("event_at_hour").over(w_event_type_ticket_id_by_even))) / 60, 1).alias("minutes_since_prev_event_at_hour"),
        F.ntile(10).over(w_ticket_id_by_event_at_hour).alias("event_at_hour_decile"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_hour_r_2).alias("first_known_event_at"),
        F.lead("event_at", 3).over(w_ticket_id_by_event_at_hour_2).alias("next3_event_at"),
    )
)

derived = (
    ranked
    .filter(F.col("event_at_hour") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("last_event_at_hour_2")) - F.unix_timestamp(F.col("last_known_event_at_hour"))) / 3600, 1).alias("hours_last_known_event_at_hour_to_last_event_at_"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.date_format(F.col("prev2_event_at"), "yyyy-MM").alias("prev2_event_at_label"),
    )
)

sequenced = (
    derived
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_ref",
        "prev2_event_at",
        "event_at_hour_rank",
        "seq_no",
        "event_at_hour_percent_rank",
        "last_event_at_hour_2",
        "event_at_hour_cume_dist",
        "minutes_since_prev_event_at_hour",
        "event_at_hour_decile",
        "next3_event_at",
        "hours_last_known_event_at_hour_to_last_event_at_",
        "prev2_event_at_label",
        F.round(F.col("event_at_hour_percent_rank") - F.avg("event_at_hour_percent_rank").over(w_event_at_hour_decile_seq_no), 2).alias("event_at_hour_percent_rank_vs_event_at_hour_deci"),
        F.round(F.col("event_type_len") - F.avg("event_type_len").over(w_event_at_hour_decile_seq_no), 2).alias("event_type_len_vs_event_at_hour_decile_seq_no_av"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_event_at_hour_decile_seq_no), F.lit(0)), 4).alias("event_type_len_share_of_event_at_hour_decile_seq"),
        F.round(F.col("event_at_hour_decile") - F.avg("event_at_hour_decile").over(w_event_at_hour_decile_seq_no), 2).alias("event_at_hour_decile_vs_event_at_hour_decile_seq"),
        F.row_number().over(w_next3_event_at_month_event_t).alias("rn"),
        F.first("last_event_at_hour").over(w_all_by_event_at_hour_rank_fu).alias("first_last_event_at_hour"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_ref",
        "prev2_event_at",
        "event_at_hour_rank",
        "seq_no",
        "event_at_hour_percent_rank",
        "last_event_at_hour_2",
        "event_at_hour_decile",
        "next3_event_at",
        "prev2_event_at_label",
        "event_at_hour_percent_rank_vs_event_at_hour_deci",
        "event_type_len_vs_event_at_hour_decile_seq_no_av",
        "event_type_len_share_of_event_at_hour_decile_seq",
        "event_at_hour_decile_vs_event_at_hour_decile_seq",
        "rn",
        "first_last_event_at_hour",
    )
    .orderBy(F.col("prev2_event_at").desc())
)
