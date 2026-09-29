from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_ticket_channel_no = Window.orderBy("pct_ticket_channel_not_in_phone", "ticket_channel")
w_n_ticket_channel_is_not_null = Window.partitionBy("n_ticket_channel_is_not_null").orderBy(F.col("pct_ticket_channel_not_in_phone").desc(), "ticket_channel").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_ticket_channel_is_not_null_2 = Window.partitionBy("n_ticket_channel_is_not_null").orderBy(F.col("pct_ticket_channel_not_in_phone").desc())

cleaned = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") != "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

exploded_detail_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "system"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

enriched_ticket = (
    exploded_detail_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_ticket_events.alias("ti"), F.col("ti.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_event_at",
        F.col("t.channel").alias("ticket_channel"),
        "ti.event_at",
    )
)

grouped = (
    enriched_ticket
    .filter(F.col("ticket_channel").isin("phone", "web", "chat"))
    .groupBy("ticket_channel")
    .agg(
        F.sum(F.when(F.col("ticket_channel").isNotNull(), 1).otherwise(0)).alias("n_ticket_channel_is_not_null"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("ticket_channel") == "phone", 1).otherwise(0)).alias("n_ticket_channel_phone"),
        F.round(100 * F.count(F.when(~F.col("ticket_channel").isin("phone", "web"), 1)) / F.count("*"), 1).alias("pct_ticket_channel_not_in_phone"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_trends = (
    grouped
    .select(
        "ticket_channel",
        "distinct_event_ids",
        "distinct_ids",
        "n_ticket_channel_phone",
        "n_rows",
        (F.col("distinct_event_ids") - F.lag("distinct_event_ids").over(w_all_by_pct_ticket_channel_no)).alias("distinct_event_ids_change"),
        F.min("n_ticket_channel_phone").over(w_n_ticket_channel_is_not_null).alias("min_n_ticket_channel_phone_so_far"),
        F.round(F.percent_rank().over(w_n_ticket_channel_is_not_null_2), 3).alias("pct_ticket_channel_not_in_phone_percent_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("n_rows") < 9)
    .select(
        "ticket_channel",
        "distinct_event_ids",
        "distinct_ids",
        "n_ticket_channel_phone",
        "min_n_ticket_channel_phone_so_far",
        F.upper(F.trim(F.col("ticket_channel"))).alias("ticket_channel_clean"),
    )
    .orderBy("ticket_channel_clean")
)
