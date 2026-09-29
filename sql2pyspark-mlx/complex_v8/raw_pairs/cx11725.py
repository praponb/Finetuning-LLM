from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_by_event_count = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_count").desc_nulls_first(), "id")
w_event_count = Window.partitionBy("event_count")
w_event_at_year_by_event_count_2 = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_count").desc_nulls_first())

filtered = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        "t.first_event_at",
    )
)

with_trends = (
    profiled
    .select(
        "*",
        F.lead("event_count_2", 3).over(w_event_at_year_by_event_count).alias("next3_event_count_2"),
        F.round(F.col("event_count") - F.avg("event_count").over(w_event_count), 2).alias("event_count_vs_event_count_avg"),
        F.rank().over(w_event_at_year_by_event_count_2).alias("event_count_rank"),
        F.lead("event_at_year").over(w_event_at_year_by_event_count).alias("next_event_at_year"),
    )
)

per_group = (
    with_trends
    .groupBy(
        "event_type",
        F.date_trunc("month", "next_event_at_year").alias("next_event_at_year_month"),
        "event_count_2",
    )
    .agg(
        F.count(F.when(F.col("actor") != "system", 1)).alias("n_actor_system"),
        F.min("next_event_at_year").alias("first_next_event_at_year"),
        F.min("event_count_2").alias("min_event_count_2"),
        F.sum(F.when(F.col("actor") == "agent", F.col("event_count_vs_event_count_avg"))).alias("event_count_vs_event_count_avg_actor_agent"),
    )
)

labeled = (
    per_group
    .select(
        "event_type",
        "next_event_at_year_month",
        "event_count_2",
        "n_actor_system",
        "min_event_count_2",
        F.concat(F.col("event_type"), F.lit("#"), F.col("n_actor_system").cast("string")).alias("event_type_ref"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown_2"),
    )
)

enriched = (
    labeled
    .select(
        "event_type",
        "next_event_at_year_month",
        "event_count_2",
        "min_event_count_2",
        "event_type_or_unknown",
        "event_type_or_unknown_2",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown_3"),
    )
)

result = (
    enriched
    .filter(F.col("next_event_at_year_month") < F.lit("2025-02-01 09:00:00").cast("timestamp"))
    .select(
        "event_type",
        "next_event_at_year_month",
        "min_event_count_2",
        "event_type_or_unknown",
        "event_type_or_unknown_2",
        "event_type_or_unknown_3",
        F.hour(F.col("next_event_at_year_month")).alias("next_event_at_year_month_hour"),
    )
    .orderBy(F.col("event_type_or_unknown_3").desc())
)
