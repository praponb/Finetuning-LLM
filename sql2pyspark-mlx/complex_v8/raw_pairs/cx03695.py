from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_n_event_type_not_i = Window.partitionBy("ticket_id", "n_event_type_not_in_replied_re").orderBy(F.col("distinct_event_types").desc_nulls_first(), "ticket_id").rowsBetween(-3, -1)
w_ticket_id_n_event_type_not_i_2 = Window.partitionBy("ticket_id", "n_event_type_not_in_replied_re")
w_ticket_id_n_event_type_not_i_3 = Window.partitionBy("ticket_id", "n_event_type_not_in_replied_re").orderBy(F.col("distinct_event_types").desc_nulls_first(), "ticket_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_event_types = Window.orderBy(F.col("distinct_event_types").desc(), "ticket_id").rowsBetween(-3, -1)
w_ticket_id_n_event_type_not_i_4 = Window.partitionBy("ticket_id", "n_event_type_not_in_replied_re").orderBy(F.col("distinct_event_types").desc_nulls_first(), "ticket_id").rowsBetween(-6, -1)

valid = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

derived = valid.withColumn("event_type_prefix", F.substring(F.trim(F.col("event_type")), 1, 3))

with_activity = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left_semi")
)

totals = (
    with_activity
    .filter(F.hour(F.col("event_at")).between(9, 11))
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.sum(F.when(~F.col("event_type").isin("replied", "resolved"), 1).otherwise(0)).alias("n_event_type_not_in_replied_re"),
    )
)

with_windows = (
    totals
    .select(
        "ticket_id",
        "n_rows",
        "n_event_type_not_in_replied_re",
        F.sum("n_rows").over(w_ticket_id_n_event_type_not_i).alias("n_rows_sum_3_prior"),
        F.max("n_rows").over(w_ticket_id_n_event_type_not_i_2).alias("ticket_id_n_event_type_not_in_replied_re_max_n_r"),
        F.nth_value("n_rows", 2).over(w_ticket_id_n_event_type_not_i_3).alias("second_n_rows"),
        F.round(F.avg("distinct_event_types").over(w_all_by_distinct_event_types), 2).alias("distinct_event_types_avg_3_prior"),
        F.round(F.avg("n_rows").over(w_ticket_id_n_event_type_not_i_4), 2).alias("n_rows_avg_6_prior"),
        F.round(F.avg("n_rows").over(w_ticket_id_n_event_type_not_i_2), 2).alias("ticket_id_n_event_type_not_in_replied_re_avg_n_r"),
    )
)

enriched_ticket = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.n_rows",
        "s.n_event_type_not_in_replied_re",
        "s.n_rows_sum_3_prior",
        "s.ticket_id_n_event_type_not_in_replied_re_max_n_r",
        "s.second_n_rows",
        "s.distinct_event_types_avg_3_prior",
        "s.n_rows_avg_6_prior",
        "s.ticket_id_n_event_type_not_in_replied_re_avg_n_r",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    enriched_ticket
    .select(
        "ticket_id",
        "n_rows",
        "n_event_type_not_in_replied_re",
        "n_rows_sum_3_prior",
        "second_n_rows",
        "n_rows_avg_6_prior",
        "ticket_id_n_event_type_not_in_replied_re_avg_n_r",
        "ticket_agent_id",
        "ticket_closed_at",
        (F.floor(F.col("n_rows_sum_3_prior") / 5) * 5).alias("n_rows_sum_3_prior_bucket"),
        F.round((F.unix_timestamp(F.col("ticket_closed_at")) - F.unix_timestamp(F.col("ticket_opened_at"))) / 60, 1).alias("minutes_ticket_opened_at_to_ticket_closed_at"),
    )
    .orderBy(F.col("n_rows_avg_6_prior").desc())
)
