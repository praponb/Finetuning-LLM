from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "event_type").orderBy("event_at_date", "id")
w_ticket_id_ticket_priority = Window.partitionBy("ticket_id", "ticket_priority")
w_all_by_ticket_csat = Window.orderBy("ticket_csat", "id")
w_event_type_by_ticket_closed = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id").rowsBetween(-2, Window.currentRow)
w_ticket_csat_actor = Window.partitionBy("ticket_csat", "actor")
w_ticket_id_by_ticket_csat_sum = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat_sum_3").desc_nulls_first(), "id")

valid = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "customer")) & (F.col("event_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined_ticket = (
    latest.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_date",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_date",
        "ticket_priority",
        "ticket_closed_at",
        "ticket_csat",
        "agent_name",
        F.round(F.avg("ticket_csat").over(w_ticket_id_ticket_priority), 2).alias("ticket_id_ticket_priority_avg_ticket_csat"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_all_by_ticket_csat))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.sum("ticket_csat").over(w_event_type_by_ticket_closed).alias("ticket_csat_sum_3"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id_ticket_priority), 2).alias("ticket_csat_vs_ticket_id_ticket_priority_avg"),
        F.sum("ticket_csat").over(w_ticket_id_ticket_priority).alias("ticket_id_ticket_priority_sum_ticket_csat"),
        F.size(F.collect_set("event_type").over(w_ticket_id_ticket_priority)).alias("distinct_event_type_in_ticket_id_ticket_priority"),
    )
)

qualified = (
    with_trends
    .crossJoin(with_trends.agg(F.max("ticket_csat_sum_3").alias("overall_max_ticket_csat_sum_3")))
    .filter(F.col("ticket_csat_sum_3") < F.col("overall_max_ticket_csat_sum_3"))
    .drop("overall_max_ticket_csat_sum_3")
)

metrics = (
    qualified
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_csat_actor).alias("rows_in_ticket_csat_actor"),
        F.ntile(10).over(w_ticket_id_by_ticket_csat_sum).alias("ticket_csat_sum_3_decile"),
    )
)

actor_stats = (
    metrics
    .groupBy("actor")
    .agg(F.max("ticket_id_ticket_priority_avg_ticket_csat").alias("max_ticket_id_ticket_priority_avg_ticket_csat"))
)

compared = (
    metrics.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.ticket_id_ticket_priority_avg_ticket_csat") == F.col("g.max_ticket_id_ticket_priority_avg_ticket_csat"))
    .select(
        "s.*",
        "g.max_ticket_id_ticket_priority_avg_ticket_csat",
        F.round(F.col("s.ticket_id_ticket_priority_avg_ticket_csat") - F.col("g.max_ticket_id_ticket_priority_avg_ticket_csat"), 2).alias("ticket_id_ticket_priority_avg_ticket_csat_minus_"),
    )
)

result = (
    compared
    .select(
        "id",
        "actor",
        "ticket_priority",
        "ticket_closed_at",
        "ticket_csat",
        "ticket_id_ticket_priority_avg_ticket_csat",
        "minutes_since_prev_ticket_closed_at",
        "ticket_csat_vs_ticket_id_ticket_priority_avg",
        "distinct_event_type_in_ticket_id_ticket_priority",
        "rows_in_ticket_csat_actor",
        "ticket_csat_sum_3_decile",
        "max_ticket_id_ticket_priority_avg_ticket_csat",
    )
    .orderBy(
        F.col("max_ticket_id_ticket_priority_avg_ticket_csat").desc(),
        F.col("id").desc_nulls_first(),
    )
)
