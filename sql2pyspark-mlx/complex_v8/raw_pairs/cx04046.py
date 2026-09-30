from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_first_known_event_at = Window.orderBy(F.col("first_known_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_event_at_2 = Window.orderBy(F.col("first_known_event_at").desc(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(7, 11)) & (F.col("t.actor") == "agent"), "left_anti")
    .filter(F.col("event_at") < F.lit("2025-02-12 11:00:00").cast("timestamp"))
)

windowed = (
    screened
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_plus_30m",
        F.lag("event_at_plus_30m").over(w_all_by_event_at).alias("prev_event_at_plus_30m"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.lead("event_at").over(w_all_by_event_at_2).alias("next_event_at"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_all_by_first_known_event_at).alias("first_known_event_at_2"),
        F.last("event_at_plus_30m").over(w_ticket_id_by_event_at_full).alias("last_event_at_plus_30m"),
        F.round(F.percent_rank().over(w_all_by_first_known_event_at_2), 3).alias("first_known_event_at_percent_rank"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") != "system"), "left_semi")
)

stats = (
    filtered
    .groupBy("event_type")
    .agg(
        F.round(F.sum("event_at_rank") / F.nullif(F.sum("first_known_event_at_percent_rank"), F.lit(0)), 3).alias("event_at_rank_to_first_known_event_at_percent_ra"),
        F.count("*").alias("cnt"),
        F.round(F.sum("first_known_event_at_percent_rank"), 2).alias("total_first_known_event_at_percent_rank"),
        F.percentile_approx("event_at_rank", 0.5).alias("median_event_at_rank"),
    )
)

result = (
    stats
    .filter(F.col("total_first_known_event_at_percent_rank") >= 1.8)
    .select(
        "event_type",
        "event_at_rank_to_first_known_event_at_percent_ra",
        "cnt",
        "total_first_known_event_at_percent_rank",
        "median_event_at_rank",
        (F.col("median_event_at_rank") + F.col("cnt")).alias("median_event_at_rank_plus_cnt"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(
        F.col("event_at_rank_to_first_known_event_at_percent_ra").desc(),
        "median_event_at_rank_plus_cnt",
    )
)
