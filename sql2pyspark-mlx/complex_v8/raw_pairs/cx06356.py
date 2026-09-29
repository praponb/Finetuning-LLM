from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_count_moving_2 = Window.orderBy(F.col("event_count").desc(), "id", "event_id").rowsBetween(-2, Window.currentRow)
w_event_event_at_to_date_by_ev = Window.partitionBy(F.to_date("event_event_at")).orderBy("event_at", "id", "event_id")
w_last_event_at_2_month_event = Window.partitionBy(F.date_trunc("month", "last_event_at_2"), F.month("event_event_at"))
w_event_actor_by_event_count_m = Window.partitionBy("event_actor").orderBy(F.col("event_count_max_3").desc(), "id", "event_id")

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at_2",
    )
)

detail_rows = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("system", "customer")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_count",
        "s.last_event_at_2",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_windows = (
    detail_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "last_event_at_2",
        "event_id",
        "event_event_at",
        "event_actor",
        F.max("event_count").over(w_all_by_event_count_moving_2).alias("event_count_max_3"),
        F.rank().over(w_event_event_at_to_date_by_ev).alias("event_at_rank"),
    )
)

tagged = (
    with_windows
    .select(
        "id",
        "event_at",
        "last_event_at_2",
        "event_id",
        "event_event_at",
        "event_actor",
        "event_count_max_3",
        (F.col("event_at_rank") - F.col("event_count_max_3")).alias("event_at_rank_minus_event_count_max_3"),
        F.round(F.col("event_count_max_3") / F.nullif(F.col("event_at_rank"), F.lit(0)), 3).alias("event_count_max_3_per_event_at_rank"),
        F.greatest(F.col("event_count_max_3"), F.col("event_at_rank")).alias("greatest_event_count_max_3_event_at_rank"),
    )
)

metrics = (
    tagged
    .withColumn("last_event_at_2_month_event_event_at_month_sum_e", F.sum("event_count_max_3").over(w_last_event_at_2_month_event))
    .withColumn("next_event_at", F.lead("event_at").over(w_event_actor_by_event_count_m))
)

result = (
    metrics
    .groupBy("last_event_at_2")
    .pivot("event_actor", ["customer", "system", "agent"])
    .agg(F.max("event_at_rank_minus_event_count_max_3"))
    .orderBy("last_event_at_2")
)
