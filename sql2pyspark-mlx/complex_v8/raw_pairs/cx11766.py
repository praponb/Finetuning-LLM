from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_event_type_count_event = Window.partitionBy("event_event_type_count", "event_count").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_count_ticket_id_by_eve = Window.partitionBy("event_count", "ticket_id").orderBy("event_at_plus_2h")
w_event_at_plus_2h_month_ticke = Window.partitionBy(F.month("event_at_plus_2h"), "ticket_id")
w_ticket_id_event_count_by_eve = Window.partitionBy("ticket_id", "event_count").orderBy("event_at", "id")
w_event_event_type_count_event_2 = Window.partitionBy("event_event_type_count", "event_count").orderBy("event_at", "id")

base_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")) & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

tagged = (
    base_events
    .filter(F.col("actor").isin("system", "agent"))
    .select(
        "*",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "replied", 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

ranked = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_plus_2h",
        "event_event_type_count",
        "event_count",
        F.nth_value("event_count", 2).over(w_event_event_type_count_event).alias("second_event_count"),
        F.round(F.percent_rank().over(w_event_count_ticket_id_by_eve), 3).alias("event_at_plus_2h_percent_rank"),
        F.round(F.col("event_count") - F.avg("event_count").over(w_event_at_plus_2h_month_ticke), 2).alias("event_count_vs_event_at_plus_2h_month_ticket_id_"),
        F.dense_rank().over(w_ticket_id_event_count_by_eve).alias("event_at_dense_rank"),
        F.row_number().over(w_event_event_type_count_event_2).alias("seq_no"),
        F.lead("event_event_type_count").over(w_event_event_type_count_event_2).alias("next_event_event_type_count"),
    )
)

result = (
    ranked
    .cube("second_event_count", "next_event_event_type_count", "actor")
    .agg(
        F.grouping("second_event_count").alias("is_total_second_event_count"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("event_at_plus_2h_percent_rank")).otherwise(0)).alias("event_at_plus_2h_percent_rank_actor_is_not_null"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("second_event_count").asc_nulls_last(),
        F.col("next_event_event_type_count").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
