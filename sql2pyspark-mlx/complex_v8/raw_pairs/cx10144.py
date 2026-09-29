from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 21)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

actor_stats = (
    valid
    .groupBy("actor")
    .agg(F.max("event_at_quarter").alias("max_event_at_quarter"))
)

compared = (
    valid.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_quarter") == F.col("g.max_event_at_quarter"))
    .select(
        "s.*",
        "g.max_event_at_quarter",
        F.round(F.col("s.event_at_quarter") - F.col("g.max_event_at_quarter"), 2).alias("event_at_quarter_minus_max"),
    )
)

latest = (
    compared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    latest
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_quarter",
        "max_event_at_quarter",
        "event_at_quarter_minus_max",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.last_event_at_2",
    )
)

tagged = (
    summarized_rows
    .select(
        "*",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.round(F.col("max_event_at_quarter") * 1.5, 2).alias("max_event_at_quarter_adj"),
    )
)

ticket_id_stats = (
    tagged
    .groupBy("ticket_id")
    .agg(F.avg("max_event_at_quarter_adj").alias("avg_max_event_at_quarter_adj"))
)

above_average = (
    tagged.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.max_event_at_quarter_adj") > F.col("g.avg_max_event_at_quarter_adj"))
    .select(
        "s.*",
        "g.avg_max_event_at_quarter_adj",
        F.round(F.col("s.max_event_at_quarter_adj") - F.col("g.avg_max_event_at_quarter_adj"), 2).alias("max_event_at_quarter_adj_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "ticket_id",
        "event_at",
        "max_event_at_quarter",
        "event_at_quarter_minus_max",
        "actor_prefix",
        "event_count",
        "last_event_at",
        "event_at_dayofweek",
        "max_event_at_quarter_adj",
        "avg_max_event_at_quarter_adj",
        F.round(F.col("event_count") / F.nullif(F.col("event_at_quarter"), F.lit(0)), 3).alias("event_count_per_event_at_quarter"),
        F.round(F.col("max_event_at_quarter") / F.nullif(F.col("event_count"), F.lit(0)), 3).alias("max_event_at_quarter_per_event_count"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
