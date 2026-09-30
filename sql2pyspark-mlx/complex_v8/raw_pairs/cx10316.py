from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_latest_2 = Window.partitionBy("event_type").orderBy(F.col("first_event_at").desc(), F.col("id").desc())

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

latest_per_key = (
    scoped_events
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.first("event_at").over(w_event_type_ticket_id_by_even).alias("first_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_ticket_id_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even_2), 3).alias("event_at_percent_rank"),
        F.ntile(5).over(w_event_type_ticket_id_by_even_2).alias("event_at_quintile"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded_detail_events = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.minutes_since_prev_event_at",
        "s.event_at_percent_rank",
        "s.event_at_quintile",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    exploded_detail_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_quintile",
        "event_event_type",
        "event_actor",
        "event_event_at",
        F.round(F.col("event_at_percent_rank") * 0.25, 2).alias("event_at_percent_rank_adj"),
        F.concat(F.col("event_actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_actor_ref"),
    )
    .orderBy(F.col("event_actor").desc())
)
