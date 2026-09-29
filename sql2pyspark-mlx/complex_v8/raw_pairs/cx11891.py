from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_priority_by = Window.partitionBy("ticket_id", "ticket_priority").orderBy("event_at", "id")
w_ticket_id_ticket_priority_by_2 = Window.partitionBy("ticket_id", "ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_ticket_id_by = Window.partitionBy("ticket_priority", "ticket_id").orderBy("event_at", "id")
w_ticket_id_ticket_priority_by_3 = Window.partitionBy("ticket_id", "ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at", "id")
w_ticket_priority_ticket_id_by_2 = Window.partitionBy("ticket_priority", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_event_at_month_by_first = Window.partitionBy(F.date_trunc("month", "last_event_at")).orderBy(F.col("first_known_event_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_prev2_event_at_year_event_at = Window.partitionBy(F.year("prev2_event_at"), F.year("event_at"))
w_distinct_event_types_last_kn = Window.partitionBy("distinct_event_types", F.month("last_known_event_at_month"))
w_distinct_event_types_by_dist = Window.partitionBy("distinct_event_types").orderBy("distinct_event_types", "event_at_month", "last_event_at_month", "last_known_event_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details_ticket = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

ranked = (
    with_details_ticket
    .select(
        "*",
        F.lag("event_at").over(w_ticket_id_ticket_priority_by).alias("prev_event_at"),
        F.last("event_at").over(w_ticket_id_ticket_priority_by_2).alias("last_event_at"),
        F.lag("event_at", 2).over(w_ticket_priority_ticket_id_by).alias("prev2_event_at"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_ticket_priority_by_3).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.last("event_at", ignorenulls=True).over(w_ticket_priority_ticket_id_by_2).alias("last_known_event_at"),
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_ticket_priority_by), 3).alias("event_at_percent_rank_2"),
    )
)

sequenced = (
    ranked
    .withColumn("event_at_percent_rank_max_7", F.max("event_at_percent_rank").over(w_last_event_at_month_by_first))
    .withColumn("event_at_percent_rank_2_vs_prev2_event_at_year_e", F.round(F.col("event_at_percent_rank_2") - F.avg("event_at_percent_rank_2").over(w_prev2_event_at_year_event_at), 2))
)

totals = (
    sequenced
    .filter(F.col("event_at_percent_rank_2_vs_prev2_event_at_year_e") <= 0.2)
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        F.date_trunc("month", "last_event_at").alias("last_event_at_month"),
        F.date_trunc("month", "last_known_event_at").alias("last_known_event_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(F.avg("event_at_percent_rank_2"), 2).alias("avg_event_at_percent_rank_2"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("event_at_percent_rank_2_vs_prev2_event_at_year_e").alias("max_event_at_percent_rank_2_vs_prev2_event_at_ye"),
    )
    .filter(F.col("max_event_at_percent_rank_2_vs_prev2_event_at_ye") > -0.8)
    .drop("max_event_at_percent_rank_2_vs_prev2_event_at_ye")
)

metrics = (
    totals
    .select(
        "*",
        F.round(F.avg("avg_event_at_percent_rank_2").over(w_distinct_event_types_last_kn), 2).alias("distinct_event_types_last_known_event_at_month_m"),
        F.nth_value("last_known_event_at_month", 2).over(w_distinct_event_types_by_dist).alias("second_last_known_event_at_month"),
        F.count(F.lit(1)).over(w_distinct_event_types_last_kn).alias("rows_in_distinct_event_types_last_known_event_at"),
    )
)

result = (
    metrics
    .select(
        "event_at_month",
        "last_known_event_at_month",
        "actor_list",
        "avg_event_at_percent_rank_2",
        "distinct_event_types",
        "distinct_event_types_last_known_event_at_month_m",
        "second_last_known_event_at_month",
        "rows_in_distinct_event_types_last_known_event_at",
        F.when(F.col("distinct_event_types") >= 9, "A").when(F.col("distinct_event_types") >= 3, "B").otherwise("C").alias("distinct_event_types_tier"),
        F.when(F.col("distinct_event_types") >= 8, "large").when(F.col("distinct_event_types") >= 4, "medium").otherwise("small").alias("distinct_event_types_tier_2"),
    )
    .orderBy(F.col("rows_in_distinct_event_types_last_known_event_at").asc())
)
