from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_at_dense_rank_filled_e = Window.partitionBy("event_at_dense_rank_filled", "event_at_dense_rank").orderBy(F.col("second_event_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_event_at_dense_rank_filled_e_2 = Window.partitionBy("event_at_dense_rank_filled", "event_at_dense_rank").orderBy(F.col("second_event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")

base_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    base_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_event_type_actor_by_event_at).alias("second_event_at"),
        F.dense_rank().over(w_event_type_actor_by_event_at_2).alias("event_at_dense_rank"),
        F.dense_rank().over(w_event_type_actor_by_event_at_3).alias("event_at_dense_rank_2"),
        F.lead("event_at", 2).over(w_event_type_by_event_at).alias("next2_event_at"),
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.coalesce(F.col("event_at_dense_rank"), F.lit(0)).alias("event_at_dense_rank_filled"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round(F.avg("event_at_percent_rank").over(w_event_at_dense_rank_filled_e), 2).alias("event_at_percent_rank_avg_2"),
        F.round(F.cume_dist().over(w_event_at_dense_rank_filled_e_2), 3).alias("second_event_at_cume_dist"),
        F.lead("second_event_at").over(w_ticket_id_by_event_at).alias("next_second_event_at"),
    )
)

top_rows = with_windows.filter(F.col("event_at_dense_rank_2") <= 3)

scored_2 = (
    top_rows
    .filter(F.col("event_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.coalesce(F.col("event_at_dense_rank"), F.lit(0)).alias("event_at_dense_rank_filled_2"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

profiled = (
    scored_2.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "second_event_at",
        "event_at_dense_rank",
        "event_at_dense_rank_2",
        "event_at_date",
        "event_at_dense_rank_filled",
        "event_at_percent_rank_avg_2",
        "next_second_event_at",
        "days_since_event_at",
        "actor_prefix",
        "event_count",
    )
    .orderBy(F.col("actor_prefix").desc_nulls_first())
)
