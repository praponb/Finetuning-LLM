from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_count_2").desc(), F.col("id").desc())

recent = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

with_stats = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

latest_per_key = (
    with_stats
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

rollup_base = (
    latest_per_key
    .groupBy(
        "event_type",
        "event_count",
        F.date_trunc("month", "event_at_week").alias("event_at_week_month"),
    )
    .agg(
        F.sum(F.when(F.col("actor").isin("system", "agent"), 1).otherwise(0)).alias("n_actor_in_system_agent"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("event_count_2")).otherwise(0)).alias("event_count_2_actor_is_not_null"),
        F.count("*").alias("cnt"),
        F.min("event_at").alias("first_event_at"),
        F.sum("event_count").alias("total_event_count"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    rollup_base
    .filter(F.col("event_count").between(2, 6))
    .select(
        "event_type",
        "event_count",
        "cnt",
        "first_event_at",
        "total_event_count",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
    .orderBy(F.col("event_type_or_unknown").desc_nulls_first(), "total_event_count")
)
