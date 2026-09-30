from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_ticket_opened_at = Window.partitionBy("actor").orderBy(F.col("ticket_opened_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_event_type_ticket_opened_at = Window.partitionBy("event_type", F.to_date("ticket_opened_at")).orderBy("ticket_opened_at", "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_by_ticket_opened_at_ti = Window.partitionBy("actor").orderBy(F.col("ticket_opened_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at")
w_event_type_ticket_opened_at_2 = Window.partitionBy("event_type", "ticket_opened_at_rank").orderBy("last_ticket_opened_at", "id")
w_ticket_opened_at_rank_event = Window.partitionBy("ticket_opened_at_rank", F.to_date("event_at"))
w_ticket_opened_at_decile_tick = Window.partitionBy("ticket_opened_at_decile", "ticket_opened_at_rank").orderBy("ticket_opened_at_seq_2")
w_event_type_ticket_opened_at_3 = Window.partitionBy("event_type", "ticket_opened_at_rank").orderBy("last_ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_opened_at_decile_tick_2 = Window.partitionBy("ticket_opened_at_decile", "ticket_opened_at_rank").orderBy("ticket_opened_at_seq_2", "id")
w_ticket_opened_at_decile_tick_3 = Window.partitionBy("ticket_opened_at_decile", "ticket_opened_at_rank").orderBy("ticket_opened_at_seq_2", "id").rowsBetween(-3, Window.currentRow)

base_events = (
    df_ticket_events
    .filter((F.col("event_type") != "replied") & (F.col("actor").isin("customer", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

enriched_ticket = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

metrics = (
    enriched_ticket
    .select(
        "*",
        F.round(F.cume_dist().over(w_actor_by_ticket_opened_at), 3).alias("ticket_opened_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.percent_rank().over(w_event_type_ticket_opened_at), 3).alias("ticket_opened_at_percent_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at).alias("first_known_event_at"),
        F.row_number().over(w_event_type_actor_by_event_at_2).alias("event_at_seq"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.row_number().over(w_actor_by_ticket_opened_at).alias("ticket_opened_at_seq"),
        F.rank().over(w_actor_by_ticket_opened_at).alias("ticket_opened_at_rank"),
        F.dense_rank().over(w_actor_by_ticket_opened_at_ti).alias("ticket_opened_at_dense_rank"),
        F.row_number().over(w_event_type_ticket_opened_at).alias("ticket_opened_at_seq_2"),
        F.last("ticket_opened_at").over(w_all_by_event_at_full).alias("last_ticket_opened_at"),
        F.ntile(10).over(w_event_type_ticket_opened_at).alias("ticket_opened_at_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_ticket_opened_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at_3), 3).alias("event_at_cume_dist_3"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.lag("event_at_cume_dist_3", 1, 0).over(w_event_type_ticket_opened_at_2).alias("prev_event_at_cume_dist_3"),
        F.count(F.lit(1)).over(w_ticket_opened_at_rank_event).alias("rows_in_ticket_opened_at_rank_event_at_to_date"),
        F.round(F.cume_dist().over(w_ticket_opened_at_decile_tick), 3).alias("ticket_opened_at_seq_2_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_event_type_ticket_opened_at_3).alias("first_known_event_at_2"),
        (F.col("minutes_since_prev_event_at") - F.lag("minutes_since_prev_event_at").over(w_ticket_opened_at_decile_tick_2)).alias("minutes_since_prev_event_at_change"),
        F.round(F.avg("ticket_opened_at_seq_2").over(w_ticket_opened_at_decile_tick_3), 2).alias("ticket_opened_at_seq_2_avg_4"),
    )
)

per_group = (
    ranked
    .filter(F.col("ticket_opened_at_seq_2") <= 7)
    .groupBy(F.year("ticket_opened_at").alias("ticket_opened_at_year"), "event_type")
    .agg(
        F.min("last_ticket_opened_at").alias("first_last_ticket_opened_at"),
        F.count("*").alias("n_rows"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum(F.when(F.col("actor") == "system", F.col("ticket_opened_at_seq"))).alias("ticket_opened_at_seq_actor_system"),
        F.round(F.stddev("ticket_opened_at_rank"), 2).alias("stddev_ticket_opened_at_rank"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
)

result = (
    per_group
    .filter(F.col("event_type") == "replied")
    .select(
        "ticket_opened_at_year",
        "event_type",
        "first_last_ticket_opened_at",
        "n_rows",
        "distinct_actors",
        "ticket_opened_at_seq_actor_system",
        "stddev_ticket_opened_at_rank",
        F.abs(F.col("distinct_actors") - 7).alias("distinct_actors_distance"),
    )
)
