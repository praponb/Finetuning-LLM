from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_actor_group_by_event_at = Window.partitionBy("actor_group").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_plus_15m = Window.orderBy(F.col("event_at_plus_15m").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_plus_15m_ful = Window.orderBy(F.col("event_at_plus_15m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_group_by_event_at_ties = Window.partitionBy("actor_group").orderBy(F.col("event_at").desc())
w_event_at_quintile = Window.partitionBy("event_at_quintile")
w_all_by_event_at_plus_15m_qui = Window.orderBy("event_at_plus_15m_quintile", "id")
w_last_known_event_at_month_by = Window.partitionBy(F.date_trunc("month", "last_known_event_at")).orderBy(F.col("event_at_plus_15m").desc(), "id")
w_event_at_dense_rank = Window.partitionBy("event_at_dense_rank")

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

sequenced = (
    scoped_events
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_actor_group_by_event_at), 3).alias("event_at_percent_rank"),
        F.rank().over(w_actor_group_by_event_at).alias("event_at_rank"),
        F.ntile(5).over(w_all_by_event_at_plus_15m).alias("event_at_plus_15m_quintile"),
        F.round((F.unix_timestamp("event_at_plus_15m") - F.unix_timestamp(F.lag("event_at_plus_15m").over(w_all_by_event_at_plus_15m))) / 60, 1).alias("minutes_since_prev_event_at_plus_15m"),
        F.round(F.cume_dist().over(w_all_by_event_at_plus_15m), 3).alias("event_at_plus_15m_cume_dist"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.ntile(5).over(w_actor_group_by_event_at).alias("event_at_quintile"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_plus_15m_ful).alias("second_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.dense_rank().over(w_actor_group_by_event_at_ties).alias("event_at_dense_rank_2"),
    )
)

top_rows = (
    sequenced
    .filter(F.col("event_at_dense_rank_2") == 1)
    .drop("event_at_dense_rank_2")
)

sequenced_2 = (
    top_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_event_at_quintile).alias("rows_in_event_at_quintile"),
        F.round(F.percent_rank().over(w_all_by_event_at_plus_15m_qui), 3).alias("event_at_plus_15m_quintile_percent_rank"),
        F.lag("event_at", 3).over(w_all_by_event_at_plus_15m_qui).alias("prev3_event_at"),
        F.size(F.collect_set("ticket_id").over(w_event_at_quintile)).alias("distinct_ticket_id_in_event_at_quintile"),
        F.lead("event_at_plus_15m_quintile").over(w_last_known_event_at_month_by).alias("next_event_at_plus_15m_quintile"),
        F.max("event_at_dense_rank").over(w_event_at_dense_rank).alias("event_at_dense_rank_max_event_at_dense_rank"),
    )
)

result = (
    sequenced_2
    .filter(F.col("second_event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_group",
        "event_at_dense_rank",
        "event_at_percent_rank",
        "minutes_since_prev_event_at_plus_15m",
        "last_event_at",
        "event_at_quintile",
        "second_event_at",
        "rows_in_event_at_quintile",
        "event_at_plus_15m_quintile_percent_rank",
        "prev3_event_at",
        "distinct_ticket_id_in_event_at_quintile",
        "next_event_at_plus_15m_quintile",
        "event_at_dense_rank_max_event_at_dense_rank",
        F.abs(F.col("event_at_rank") - 4).alias("event_at_rank_distance"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy("id")
)
