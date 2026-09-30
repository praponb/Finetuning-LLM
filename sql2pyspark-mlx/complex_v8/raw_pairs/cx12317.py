from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_event_at_4 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc())
w_actor_event_type_by_event_at_5 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_6 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_2_by_min = Window.partitionBy("event_at_dense_rank_2").orderBy("minutes_since_prev_event_at", "id")
w_next2_event_at_2_to_date_eve = Window.partitionBy(F.to_date("next2_event_at_2"), "event_at_dense_rank")
w_all_by_event_at_dense_rank = Window.orderBy(F.col("event_at_dense_rank").desc(), "id")
w_event_at_dense_rank_2_by_min_2 = Window.partitionBy("event_at_dense_rank_2").orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_dense_rank_2_distin = Window.partitionBy("event_at_dense_rank_2", "distinct_actors")
w_all_by_min_minutes_since_pre = Window.orderBy(F.col("min_minutes_since_prev_event_at").desc(), "event_at_dense_rank_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

ranked = (
    enriched_ticket
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at", 2).over(w_actor_event_type_by_event_at).alias("next2_event_at"),
        F.lead("event_at", 2).over(w_actor_event_type_by_event_at_2).alias("next2_event_at_2"),
        F.first("event_at", ignorenulls=True).over(w_actor_event_type_by_event_at_3).alias("first_known_event_at"),
        F.dense_rank().over(w_actor_event_type_by_event_at_2).alias("event_at_dense_rank"),
        F.dense_rank().over(w_actor_event_type_by_event_at_4).alias("event_at_dense_rank_2"),
        F.first("event_at").over(w_actor_event_type_by_event_at_5).alias("first_event_at"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_4), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_actor_event_type_by_event_at_6).alias("last_event_at"),
    )
)

leaders = ranked.filter(F.col("event_at_dense_rank") == 1)

sequenced = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "agent_name",
        "minutes_since_prev_event_at",
        "next2_event_at",
        "next2_event_at_2",
        "event_at_dense_rank_2",
        "first_event_at",
        "last_event_at",
        F.row_number().over(w_event_at_dense_rank_2_by_min).alias("rn"),
        F.sum("event_at_dense_rank").over(w_next2_event_at_2_to_date_eve).alias("next2_event_at_2_to_date_event_at_dense_rank_sum"),
        F.ntile(10).over(w_all_by_event_at_dense_rank).alias("event_at_dense_rank_decile"),
        F.last("first_event_at", ignorenulls=True).over(w_event_at_dense_rank_2_by_min_2).alias("last_known_first_event_at"),
    )
)

totals = (
    sequenced
    .groupBy("event_at_dense_rank_2")
    .agg(
        F.sum("rn").alias("total_rn"),
        F.max("next2_event_at_2").alias("last_next2_event_at_2"),
        F.round(F.sum("event_at_dense_rank_decile") / F.nullif(F.sum("event_at_dense_rank_2"), F.lit(0)), 3).alias("event_at_dense_rank_decile_to_event_at_dense_ran"),
        F.min("minutes_since_prev_event_at").alias("min_minutes_since_prev_event_at"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.avg("event_at_dense_rank_2").alias("avg_event_at_dense_rank_2_check"),
    )
    .filter(F.col("avg_event_at_dense_rank_2_check") > 4)
    .drop("avg_event_at_dense_rank_2_check")
)

with_windows = (
    totals
    .select(
        "*",
        F.round(F.col("event_at_dense_rank_2") - F.avg("event_at_dense_rank_2").over(w_event_at_dense_rank_2_distin), 2).alias("event_at_dense_rank_2_vs_event_at_dense_rank_2_d"),
        F.count(F.lit(1)).over(w_all_by_min_minutes_since_pre).alias("running_count"),
    )
)

result = (
    with_windows
    .select(
        "event_at_dense_rank_2",
        "total_rn",
        "last_next2_event_at_2",
        "event_at_dense_rank_decile_to_event_at_dense_ran",
        "min_minutes_since_prev_event_at",
        "distinct_actors",
        "running_count",
        (F.floor(F.col("running_count") / 5) * 5).alias("running_count_bucket"),
    )
    .orderBy(F.col("event_at_dense_rank_decile_to_event_at_dense_ran").asc())
)
