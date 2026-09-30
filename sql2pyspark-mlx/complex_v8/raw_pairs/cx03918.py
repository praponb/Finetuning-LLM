from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_type_or_unk = Window.partitionBy("event_type", "event_type_or_unknown").orderBy("ticket_closed_at", "id")
w_all_by_ticket_closed_at_ties = Window.orderBy(F.col("ticket_closed_at").desc())
w_event_type_event_type_or_unk_2 = Window.partitionBy("event_type", "event_type_or_unknown").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_closed_at = Window.partitionBy("actor").orderBy("ticket_closed_at", "id")
w_actor_by_ticket_closed_at_ti = Window.partitionBy("actor").orderBy("ticket_closed_at")
w_event_type_event_type_or_unk_3 = Window.partitionBy("event_type", "event_type_or_unknown").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_closed_at_full = Window.orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_ticket_closed_at_cu = Window.partitionBy("actor").orderBy(F.col("ticket_closed_at_cume_dist").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_ticket_closed_at_month_by_ti = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at_dense_rank", "id").rowsBetween(-3, -1)
w_event_type_or_unknown_ticket = Window.partitionBy("event_type_or_unknown", "ticket_closed_at_seq")
w_actor_by_ticket_closed_at_cu_2 = Window.partitionBy("actor").orderBy(F.col("ticket_closed_at_cume_dist").desc_nulls_first())
w_ticket_closed_at_month_by_ti_2 = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at_dense_rank", "id")
w_event_type_ticket_closed_at = Window.partitionBy("event_type", "ticket_closed_at_rank")
w_next_event_at_year_first_tic = Window.partitionBy(F.year("next_event_at"), F.date_trunc("month", "first_ticket_closed_at")).orderBy("first_ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter((~F.col("actor").isin("customer", "agent")) & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_parent_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.closed_at").alias("ticket_closed_at"))
)

sequenced = (
    with_parent_ticket
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_event_type_or_unk), 3).alias("ticket_closed_at_percent_rank"),
        F.dense_rank().over(w_all_by_ticket_closed_at_ties).alias("ticket_closed_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_type_event_type_or_unk_2).alias("first_known_event_at"),
        F.round(F.cume_dist().over(w_event_type_event_type_or_unk), 3).alias("ticket_closed_at_cume_dist"),
        F.row_number().over(w_actor_by_ticket_closed_at).alias("ticket_closed_at_seq"),
        F.rank().over(w_actor_by_ticket_closed_at_ti).alias("ticket_closed_at_rank"),
        F.lead("event_at").over(w_actor_by_ticket_closed_at).alias("next_event_at"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_actor_by_ticket_closed_at))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.rank().over(w_event_type_event_type_or_unk).alias("ticket_closed_at_rank_2"),
        F.first("ticket_closed_at").over(w_event_type_event_type_or_unk_3).alias("first_ticket_closed_at"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_event_type_event_type_or_unk_2).alias("first_known_ticket_closed_at"),
        F.lag("event_at").over(w_event_type_event_type_or_unk).alias("prev_event_at"),
        F.last("event_at").over(w_event_type_event_type_or_unk_3).alias("last_event_at"),
        F.first("event_at").over(w_all_by_ticket_closed_at_full).alias("first_event_at"),
    )
)

labeled = sequenced.withColumn("event_type_clean", F.initcap(F.trim(F.col("event_type"))))

with_trends = (
    labeled
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "ticket_closed_at",
        "ticket_closed_at_percent_rank",
        "ticket_closed_at_dense_rank",
        "first_known_event_at",
        "ticket_closed_at_cume_dist",
        "ticket_closed_at_seq",
        "ticket_closed_at_rank",
        "next_event_at",
        "ticket_closed_at_rank_2",
        "first_known_ticket_closed_at",
        "prev_event_at",
        "last_event_at",
        "first_event_at",
        "event_type_clean",
        F.sum("ticket_closed_at_rank_2").over(w_actor_by_ticket_closed_at_cu).alias("ticket_closed_at_rank_2_sum_4"),
        F.max("ticket_closed_at_dense_rank").over(w_ticket_closed_at_month_by_ti).alias("ticket_closed_at_dense_rank_max_3_prior"),
        F.size(F.collect_set("actor").over(w_event_type_or_unknown_ticket)).alias("distinct_actor_in_event_type_or_unknown_ticket_c"),
        F.rank().over(w_actor_by_ticket_closed_at_cu_2).alias("ticket_closed_at_cume_dist_rank"),
        F.dense_rank().over(w_ticket_closed_at_month_by_ti_2).alias("ticket_closed_at_dense_rank_dense_rank"),
        F.round(F.avg("ticket_closed_at_dense_rank").over(w_event_type_ticket_closed_at), 2).alias("event_type_ticket_closed_at_rank_avg_ticket_clos"),
        F.min("ticket_closed_at_rank_2").over(w_next_event_at_year_first_tic).alias("min_ticket_closed_at_rank_2_so_far"),
    )
)

result = (
    with_trends
    .filter(F.col("distinct_actor_in_event_type_or_unknown_ticket_c").between(1, 7))
    .select(
        "id",
        "actor",
        "event_type_or_unknown",
        "ticket_closed_at",
        "ticket_closed_at_percent_rank",
        "ticket_closed_at_dense_rank",
        "first_known_event_at",
        "ticket_closed_at_cume_dist",
        "ticket_closed_at_seq",
        "ticket_closed_at_rank_2",
        "prev_event_at",
        "last_event_at",
        "first_event_at",
        "event_type_clean",
        "ticket_closed_at_cume_dist_rank",
        "ticket_closed_at_dense_rank_dense_rank",
        "event_type_ticket_closed_at_rank_avg_ticket_clos",
        "min_ticket_closed_at_rank_2_so_far",
    )
    .orderBy(
        F.col("first_event_at").asc_nulls_last(),
        "ticket_closed_at_percent_rank",
        "id",
    )
    .limit(10)
)
