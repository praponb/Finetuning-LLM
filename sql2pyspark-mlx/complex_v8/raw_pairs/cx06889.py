from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_event_type_is_not_n = Window.orderBy("n_event_type_is_not_null", "ticket_channel").rowsBetween(-2, -1)
w_all_by_n_event_type_is_not_n_2 = Window.orderBy("n_event_type_is_not_null", "ticket_channel").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_event_type_is_not_n_3 = Window.orderBy("n_event_type_is_not_null", "ticket_channel")

prepared = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_details_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_prefix",
        "s.actor_slug",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

rollup_base = (
    with_details_ticket
    .filter(F.col("ticket_id") <= 23)
    .groupBy("ticket_channel")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.array_sort(F.collect_set("ticket_channel")).alias("ticket_channel_list"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
)

with_trends = (
    rollup_base
    .select(
        "*",
        F.max("n_event_type_is_not_null").over(w_all_by_n_event_type_is_not_n).alias("n_event_type_is_not_null_max_2_prior"),
        F.nth_value("n_event_type_is_not_null", 2).over(w_all_by_n_event_type_is_not_n_2).alias("second_n_event_type_is_not_null"),
        F.ntile(5).over(w_all_by_n_event_type_is_not_n_3).alias("n_event_type_is_not_null_quintile"),
    )
)

result = (
    with_trends
    .filter(F.col("ticket_channel") == "chat")
    .select(
        "ticket_channel",
        "event_type_list",
        "ticket_channel_list",
        "n_event_type_is_not_null",
        "n_event_type_is_not_null_max_2_prior",
        "second_n_event_type_is_not_null",
        "n_event_type_is_not_null_quintile",
        F.coalesce(F.col("n_event_type_is_not_null"), F.lit(0)).alias("n_event_type_is_not_null_filled"),
    )
    .orderBy(
        "n_event_type_is_not_null",
        F.col("n_event_type_is_not_null_max_2_prior").desc(),
    )
)
