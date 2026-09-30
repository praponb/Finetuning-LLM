from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")

recent_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 13)) | (F.col("ticket_id") <= 27))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist_2"),
    )
)

event_type_stats = (
    sequenced
    .groupBy("event_type")
    .agg(F.avg("event_at_rank").alias("avg_event_at_rank"))
)

vs_group = (
    sequenced.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_rank") > F.col("g.avg_event_at_rank"))
    .select(
        "s.*",
        "g.avg_event_at_rank",
        F.round(F.col("s.event_at_rank") - F.col("g.avg_event_at_rank"), 2).alias("event_at_rank_minus_avg"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("event_event_type_count"),
    )
)

profiled = (
    vs_group.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

actor_stats = (
    profiled
    .groupBy("actor")
    .agg(F.max("avg_event_at_rank").alias("max_avg_event_at_rank"))
)

vs_group_2 = (
    profiled.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.avg_event_at_rank") == F.col("g.max_avg_event_at_rank"))
    .select(
        "s.*",
        "g.max_avg_event_at_rank",
        F.round(F.col("s.avg_event_at_rank") - F.col("g.max_avg_event_at_rank"), 2).alias("avg_event_at_rank_minus_max"),
    )
)

result = (
    vs_group_2
    .filter(F.hour(F.col("last_known_event_at")).between(8, 12))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_seq",
        "event_at_rank",
        "last_known_event_at",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "avg_event_at_rank",
        "event_at_rank_minus_avg",
        "last_event_at",
        "first_event_at",
        "event_event_type_count",
        "max_avg_event_at_rank",
        "avg_event_at_rank_minus_max",
    )
)
