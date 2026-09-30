from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at_3 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) & (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix_2"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix_3"),
    )
)

latest = (
    prepared_events
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    latest
    .select(
        "*",
        F.ntile(3).over(w_actor_by_event_at).alias("event_at_tercile"),
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("second_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.lead("event_at", 3).over(w_ticket_id_actor_by_event_at_3).alias("next3_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_stats = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count_2"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

annotated = (
    summarized_rows
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("second_event_at"))) / 3600, 1).alias("hours_second_event_at_to_event_at"),
        F.round(F.col("event_at_cume_dist") * 0.9, 2).alias("event_at_cume_dist_adj"),
    )
)

event_stats_3 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count_3"),
        F.count(F.when(F.col("event_type") == "replied", 1)).alias("event_event_type_count_2"),
    )
)

with_counts = (
    annotated.alias("s")
    .join(event_stats_3.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count_3"), F.lit(0)).alias("event_count_3"),
        F.coalesce(F.col("t.event_event_type_count_2"), F.lit(0)).alias("event_event_type_count_2"),
    )
)

result = (
    with_counts
    .filter(F.col("event_count_2") >= 1)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "actor_prefix_2",
        "actor_prefix_3",
        "event_at_tercile",
        "second_event_at",
        "event_at_cume_dist",
        "next3_event_at",
        "event_count",
        "event_count_2",
        "event_event_type_count",
        "event_count_3",
        "event_event_type_count_2",
    )
    .orderBy(F.col("actor_prefix").desc())
)
