from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_agent_id = Window.partitionBy("ticket_agent_id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_all_by_ticket_requester_id_r = Window.orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_requester_id_m = Window.orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_all_by_ticket_requester_id_r_2 = Window.orderBy(F.col("ticket_requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_all_by_ticket_requester_id = Window.orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id")
w_all_by_ticket_requester_id_2 = Window.orderBy(F.col("ticket_requester_id").asc_nulls_last(), "id")
w_all_by_ticket_requester_id_f = Window.orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_requester_id_c = Window.orderBy(F.col("ticket_requester_id_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id = Window.partitionBy("ticket_id")
w_event_at_dense_rank_ticket_a = Window.partitionBy("event_at_dense_rank", "ticket_agent_id").orderBy(F.col("ticket_requester_id_change").desc(), "id")
w_all_by_ticket_requester_id_c_2 = Window.orderBy(F.col("ticket_requester_id_change").desc(), "id").rowsBetween(-2, -1)
w_all_by_ticket_requester_id_c_3 = Window.orderBy(F.col("ticket_requester_id_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_requester_id_c_4 = Window.orderBy(F.col("ticket_requester_id_change").desc(), "id")

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

combined_details_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_prefix",
        "s.event_at_plus_1d",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_trends = (
    combined_details_ticket
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_agent_id).alias("rows_in_ticket_agent_id"),
        F.row_number().over(w_ticket_id_actor_by_event_at).alias("event_at_seq"),
        F.first("event_at", ignorenulls=True).over(w_all_by_ticket_requester_id_r).alias("first_known_event_at"),
        F.max("ticket_requester_id").over(w_all_by_ticket_requester_id_m).alias("ticket_requester_id_max_4"),
        F.dense_rank().over(w_ticket_id_actor_by_event_at_2).alias("event_at_dense_rank"),
        F.first("event_at_plus_1d", ignorenulls=True).over(w_all_by_ticket_requester_id_r_2).alias("first_known_event_at_plus_1d"),
        F.sum("ticket_requester_id").over(w_event_at_to_date).alias("event_at_to_date_sum_ticket_requester_id"),
        F.round(F.cume_dist().over(w_all_by_ticket_requester_id), 3).alias("ticket_requester_id_cume_dist"),
        (F.col("ticket_requester_id") - F.lag("ticket_requester_id").over(w_ticket_id_actor_by_event_at)).alias("ticket_requester_id_change"),
        F.round((F.unix_timestamp("event_at_plus_1d") - F.unix_timestamp(F.lag("event_at_plus_1d").over(w_all_by_ticket_requester_id_2))) / 60, 1).alias("minutes_since_prev_event_at_plus_1d"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_at_to_date), 2).alias("ticket_requester_id_vs_event_at_to_date_avg"),
        F.last("ticket_requester_id").over(w_all_by_ticket_requester_id_f).alias("last_ticket_requester_id"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("event_at_dense_rank") <= 2) & (F.col("ticket_requester_id").between(2, 24)))
)

with_trends_2 = (
    top_per_group
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_plus_1d",
        "ticket_requester_id",
        "rows_in_ticket_agent_id",
        "first_known_event_at",
        "ticket_requester_id_max_4",
        "event_at_dense_rank",
        "first_known_event_at_plus_1d",
        "ticket_requester_id_cume_dist",
        "minutes_since_prev_event_at_plus_1d",
        "ticket_requester_id_vs_event_at_to_date_avg",
        "last_ticket_requester_id",
        F.sum("event_at_seq").over(w_all_by_ticket_requester_id_c).alias("running_event_at_seq"),
        F.count(F.lit(1)).over(w_ticket_id).alias("rows_in_ticket_id"),
        F.row_number().over(w_event_at_dense_rank_ticket_a).alias("ticket_requester_id_change_seq"),
        F.sum("ticket_requester_id_change").over(w_all_by_ticket_requester_id_c_2).alias("ticket_requester_id_change_sum_2_prior"),
        F.max("ticket_requester_id_max_4").over(w_ticket_id).alias("ticket_id_max_ticket_requester_id_max_4"),
        F.last("event_at_dense_rank").over(w_all_by_ticket_requester_id_c_3).alias("last_event_at_dense_rank"),
        F.ntile(10).over(w_all_by_ticket_requester_id_c_4).alias("ticket_requester_id_change_decile"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_plus_1d",
        "ticket_requester_id",
        "rows_in_ticket_agent_id",
        "first_known_event_at",
        "ticket_requester_id_max_4",
        "minutes_since_prev_event_at_plus_1d",
        "ticket_requester_id_vs_event_at_to_date_avg",
        "rows_in_ticket_id",
        "ticket_id_max_ticket_requester_id_max_4",
        "last_event_at_dense_rank",
        "ticket_requester_id_change_decile",
        F.concat(F.col("event_type"), F.lit("#"), F.col("running_event_at_seq").cast("string")).alias("event_type_ref"),
    )
    .orderBy(
        "ticket_requester_id_change_decile",
        F.col("ticket_id_max_ticket_requester_id_max_4").desc_nulls_first(),
    )
)
