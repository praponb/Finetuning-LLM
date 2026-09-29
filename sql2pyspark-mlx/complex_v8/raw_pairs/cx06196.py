from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_at_month = Window.partitionBy("event_type", F.date_trunc("month", "event_at"))
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last())

recent = (
    df_ticket_events
    .filter(F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

with_details_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.csat").alias("ticket_csat"))
)

metrics = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_ref",
        "ticket_csat",
        F.count(F.lit(1)).over(w_event_type_event_at_month).alias("rows_in_event_type_event_at_month"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
    )
)

actor_stats = (
    metrics
    .groupBy("actor")
    .agg(F.max("event_at_rank").alias("max_event_at_rank"))
)

compared = (
    metrics.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_rank") == F.col("g.max_event_at_rank"))
    .select(
        "s.*",
        "g.max_event_at_rank",
        F.round(F.col("s.event_at_rank") - F.col("g.max_event_at_rank"), 2).alias("event_at_rank_minus_max"),
    )
)

result = (
    compared
    .cube("actor", "ticket_csat")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.count(F.when(F.col("actor") == "customer", 1)).alias("n_actor_customer"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "ticket_csat",
        "row_count",
        "distinct_actors",
        "n_actor_customer",
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("ticket_csat").asc_nulls_last())
)
