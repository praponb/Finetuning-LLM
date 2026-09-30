from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_len_ticket_id_by = Window.partitionBy("event_type_len", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_len_ticket_id_by_2 = Window.partitionBy("event_type_len", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(-6, -1)
w_event_type_len = Window.partitionBy("event_type_len")
w_event_type_len_ticket_id_by_3 = Window.partitionBy("event_type_len", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_len_ticket_id_by_4 = Window.partitionBy("event_type_len", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id")

recent = (
    df_ticket_events
    .filter((F.col("actor") == "customer") | (F.hour(F.col("event_at")).between(7, 12)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

tagged = (
    recent
    .withColumn("event_at_plus_30m_label", F.date_format(F.col("event_at_plus_30m"), "yyyy-MM"))
)

joined = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_group",
        "s.event_at_plus_30m",
        "s.event_type_len",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

profiled = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

ranked = (
    profiled
    .select(
        "*",
        F.first("ticket_opened_at").over(w_event_type_len_ticket_id_by).alias("first_ticket_opened_at"),
        F.round(F.avg("event_count").over(w_event_type_len_ticket_id_by_2), 2).alias("event_count_avg_6_prior"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_event_type_len), F.lit(0)), 4).alias("event_count_share_of_event_type_len"),
        F.max("event_count").over(w_event_type_len_ticket_id_by_3).alias("max_event_count_so_far"),
        F.lag("event_count", 3).over(w_event_type_len_ticket_id_by_4).alias("prev3_event_count"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_group",
        "event_at_plus_30m",
        "event_type_len",
        "event_count",
        "event_count_avg_6_prior",
        "event_count_share_of_event_type_len",
        "max_event_count_so_far",
        "prev3_event_count",
        F.date_trunc("hour", F.col("event_at_plus_30m")).alias("event_at_plus_30m_hour"),
    )
    .orderBy(F.col("max_event_count_so_far").asc(), "event_count")
)
