from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy("event_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_event_type_by_event_at = Window.partitionBy("event_event_type").orderBy(F.col("event_at").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id", "event_id")
w_all_by_event_at = Window.orderBy("event_at", "id", "event_id")

valid_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

exploded_detail = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "customer"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

ranked = (
    exploded_detail
    .select(
        "*",
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_event_type_by_event_at).alias("first_known_event_at"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank"),
        F.lag("first_known_event_at", 3).over(w_all_by_event_at).alias("prev3_first_known_event_at"),
    )
)

totals = (
    metrics
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_actor",
        "actor",
    )
    .agg(
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.count("*").alias("n_rows"),
        F.countDistinct("event_event_type").alias("distinct_event_event_types"),
        F.sum(F.when(F.col("event_actor").isin("agent", "system"), F.col("event_at_percent_rank")).otherwise(0)).alias("event_at_percent_rank_event_actor_in_agent_syste"),
        F.min("event_at_percent_rank").alias("min_event_at_percent_rank"),
        F.sum("event_at_percent_rank").alias("total_event_at_percent_rank"),
    )
)

scored = (
    totals
    .filter(F.month(F.col("event_at_month")).isin(2, 3))
    .select(
        "event_at_month",
        "event_actor",
        "actor",
        "distinct_ticket_ids",
        "distinct_event_event_types",
        "event_at_percent_rank_event_actor_in_agent_syste",
        "min_event_at_percent_rank",
        F.substring(F.trim(F.col("event_actor")), 1, 3).alias("event_actor_prefix"),
        F.dayofweek(F.col("event_at_month")).alias("event_at_month_dayofweek"),
        F.concat(F.col("event_actor"), F.lit("#"), F.col("n_rows").cast("string")).alias("event_actor_ref"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.avg("distinct_ticket_ids").alias("overall_avg_distinct_ticket_ids")))
    .filter(F.col("distinct_ticket_ids") > F.col("overall_avg_distinct_ticket_ids"))
    .drop("overall_avg_distinct_ticket_ids")
)

result = (
    screened
    .select(
        "event_at_month",
        "actor",
        "distinct_ticket_ids",
        "distinct_event_event_types",
        "event_at_percent_rank_event_actor_in_agent_syste",
        "min_event_at_percent_rank",
        "event_actor_prefix",
        "event_at_month_dayofweek",
        F.concat(F.col("event_actor_ref"), F.lit("#"), F.col("event_at_month_dayofweek").cast("string")).alias("event_actor_ref_ref"),
    )
    .orderBy(
        F.col("event_actor_prefix").asc_nulls_last(),
        "distinct_event_event_types",
    )
)
