from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_group_ticket_prio = Window.partitionBy("event_type_group", "ticket_priority").orderBy("event_at", "id")
w_event_type_group_ticket_prio_2 = Window.partitionBy("event_type_group", "ticket_priority").orderBy("event_at")
w_ticket_priority_by_event_at_2 = Window.partitionBy("ticket_priority").orderBy("event_at", "id")
w_ticket_priority_by_event_at_3 = Window.partitionBy("ticket_priority").orderBy("event_at")
w_event_type_group_ticket_prio_3 = Window.partitionBy("event_type_group", "ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_by_second_ev = Window.partitionBy("ticket_priority").orderBy(F.col("second_event_at").desc(), "id")
w_event_at_decile_by_minutes_s = Window.partitionBy("event_at_decile").orderBy("minutes_since_prev_event_at", "id")
w_event_at_decile_by_minutes_s_2 = Window.partitionBy("event_at_decile").orderBy("minutes_since_prev_event_at", "id").rowsBetween(-6, Window.currentRow)
w_event_at_quintile = Window.partitionBy("event_at_quintile")
w_actor = Window.partitionBy("actor")
w_all_by_minutes_since_prev_ev = Window.orderBy(F.col("minutes_since_prev_event_at").desc())
w_ticket_id_event_at_rank_2_by = Window.partitionBy("ticket_id", "event_at_rank_2").orderBy("event_at_rank_3", "id")

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isin("reopened", "assigned")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("escalated", "replied"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

enriched = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.priority").alias("ticket_priority"))
)

windowed = (
    enriched
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_priority_by_event_at).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at").over(w_event_type_by_event_at_full).alias("first_event_at"),
        F.lag("event_at").over(w_event_type_group_ticket_prio).alias("prev_event_at"),
        F.rank().over(w_event_type_group_ticket_prio_2).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_priority_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.rank().over(w_ticket_priority_by_event_at_3).alias("event_at_rank_2"),
        F.rank().over(w_event_type_group_ticket_prio).alias("event_at_rank_3"),
        F.first("event_at", ignorenulls=True).over(w_event_type_group_ticket_prio_3).alias("first_known_event_at"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.ntile(10).over(w_event_type_group_ticket_prio).alias("event_at_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_group_ticket_prio))) / 60, 1).alias("minutes_since_prev_event_at_3"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_4"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.ntile(5).over(w_ticket_priority_by_second_ev).alias("second_event_at_quintile"),
        F.lead("event_at_rank_2").over(w_event_at_decile_by_minutes_s).alias("next_event_at_rank_2"),
        F.sum("event_at_rank_4").over(w_event_at_decile_by_minutes_s_2).alias("event_at_rank_4_sum_7"),
        F.sum("event_at_quintile").over(w_event_at_quintile).alias("event_at_quintile_sum_event_at_quintile"),
        F.round(F.col("event_at_rank") / F.nullif(F.sum("event_at_rank").over(w_actor), F.lit(0)), 4).alias("event_at_rank_share_of_actor"),
        F.dense_rank().over(w_all_by_minutes_since_prev_ev).alias("minutes_since_prev_event_at_dense_rank"),
        F.round(F.cume_dist().over(w_ticket_id_event_at_rank_2_by), 3).alias("event_at_rank_3_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_ref",
        "ticket_priority",
        "second_event_at",
        "minutes_since_prev_event_at",
        "first_event_at",
        "prev_event_at",
        "event_at_rank",
        "event_at_rank_3",
        "first_known_event_at",
        "event_at_quintile",
        "minutes_since_prev_event_at_3",
        "event_at_rank_4",
        "second_event_at_quintile",
        "next_event_at_rank_2",
        "event_at_quintile_sum_event_at_quintile",
        "event_at_rank_share_of_actor",
        "minutes_since_prev_event_at_dense_rank",
        "event_at_rank_3_cume_dist",
    )
    .orderBy(F.col("event_at_rank_3").desc(), "id")
    .limit(20)
)
