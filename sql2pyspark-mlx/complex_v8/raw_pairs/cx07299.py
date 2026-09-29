from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = df_ticket_events.filter(F.col("actor").isin("customer", "agent"))

ranked = (
    prepared_events
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

ticket_id_stats = (
    ranked
    .groupBy("ticket_id")
    .agg(F.max("event_at_cume_dist").alias("max_event_at_cume_dist"))
)

compared = (
    ranked.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_at_cume_dist") == F.col("g.max_event_at_cume_dist"))
    .select(
        "s.*",
        "g.max_event_at_cume_dist",
        F.round(F.col("s.event_at_cume_dist") - F.col("g.max_event_at_cume_dist"), 2).alias("event_at_cume_dist_minus_max"),
    )
)

tagged = (
    compared
    .filter(F.col("event_at_cume_dist_minus_max").between(-0.4, 0.9))
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.round(F.col("event_at_cume_dist") * 1.1, 2).alias("event_at_cume_dist_adj"),
    )
)

totals = (
    tagged
    .groupBy("event_at_dense_rank")
    .agg(
        F.max("event_at_cume_dist").alias("max_event_at_cume_dist_2"),
        F.round(F.avg("event_at_cume_dist_adj"), 2).alias("avg_event_at_cume_dist_adj"),
        F.avg("max_event_at_cume_dist").alias("avg_max_event_at_cume_dist_check"),
    )
    .filter(F.col("avg_max_event_at_cume_dist_check") > 0.2)
    .drop("avg_max_event_at_cume_dist_check")
)

result = (
    totals
    .select(
        "event_at_dense_rank",
        "max_event_at_cume_dist_2",
        F.abs(F.col("event_at_dense_rank") - 4).alias("event_at_dense_rank_distance"),
        F.abs(F.col("event_at_dense_rank") - 6).alias("event_at_dense_rank_distance_2"),
    )
    .orderBy(
        F.col("max_event_at_cume_dist_2").asc_nulls_last(),
        "event_at_dense_rank_distance",
    )
)
