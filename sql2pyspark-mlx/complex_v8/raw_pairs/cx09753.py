from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_event_at_quarte = Window.orderBy("total_event_at_quarter", "event_type", "event_at_quarter", "actor").rowsBetween(-3, -1)
w_cnt = Window.partitionBy("cnt")
w_all_by_total_event_at_quarte_2 = Window.orderBy("total_event_at_quarter", "event_type", "event_at_quarter", "actor")

prepared_events = (
    df_ticket_events
    .filter((F.col("actor") != "system") & (F.col("event_type") == "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    prepared_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.min("event_at").alias("first_event_at_3"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        "t.first_event_at_3",
    )
)

ticket_id_stats = (
    profiled
    .groupBy("ticket_id")
    .agg(F.avg("event_at_quarter").alias("avg_event_at_quarter"))
)

compared = (
    profiled.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_at_quarter") > F.col("g.avg_event_at_quarter"))
    .select(
        "s.*",
        "g.avg_event_at_quarter",
        F.round(F.col("s.event_at_quarter") - F.col("g.avg_event_at_quarter"), 2).alias("event_at_quarter_minus_avg"),
    )
)

per_group = (
    compared
    .groupBy("event_type", "event_at_quarter", "actor")
    .agg(
        F.sum(F.when(F.col("event_type").isin("assigned", "resolved"), 1).otherwise(0)).alias("n_event_type_in_assigned_resol"),
        F.count("*").alias("cnt"),
        F.sum("event_at_quarter").alias("total_event_at_quarter"),
        F.sum(F.when(F.col("event_type").isin("replied", "escalated", "reopened"), F.col("event_at_quarter_minus_avg"))).alias("event_at_quarter_minus_avg_event_type_in_replied"),
        F.round(F.sum("event_count") / F.nullif(F.sum("avg_event_at_quarter"), F.lit(0)), 3).alias("event_count_to_avg_event_at_quarter_ratio"),
    )
)

ranked = (
    per_group
    .select(
        "event_type",
        "event_at_quarter",
        "actor",
        "cnt",
        "total_event_at_quarter",
        "event_at_quarter_minus_avg_event_type_in_replied",
        F.max("event_at_quarter").over(w_all_by_total_event_at_quarte).alias("event_at_quarter_max_3_prior"),
        F.size(F.collect_set("event_type").over(w_cnt)).alias("distinct_event_type_in_cnt"),
        F.round(F.cume_dist().over(w_all_by_total_event_at_quarte_2), 3).alias("total_event_at_quarter_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "event_type",
        "event_at_quarter",
        "actor",
        "cnt",
        "total_event_at_quarter",
        "event_at_quarter_minus_avg_event_type_in_replied",
        "event_at_quarter_max_3_prior",
        "distinct_event_type_in_cnt",
        "total_event_at_quarter_cume_dist",
    )
    .orderBy("distinct_event_type_in_cnt")
)
