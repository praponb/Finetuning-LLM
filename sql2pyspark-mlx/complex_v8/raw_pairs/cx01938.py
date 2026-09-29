from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_len_by_event_at_d = Window.partitionBy("event_type_len").orderBy("event_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id = Window.partitionBy("ticket_id")
w_event_type_by_event_type_len = Window.partitionBy("event_type").orderBy("event_type_len", "id")

recent = (
    df_ticket_events
    .filter((F.col("event_type") != "escalated") & (F.col("actor").isin("system", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

joined = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_at_date",
        "event_type_len",
        "ticket_channel",
        "ticket_csat",
        F.max("ticket_csat").over(w_event_type_len_by_event_at_d).alias("max_ticket_csat_so_far"),
        F.round(F.avg("ticket_csat").over(w_ticket_id), 2).alias("ticket_id_avg_ticket_csat"),
        F.last("event_type_len", ignorenulls=True).over(w_event_type_len_by_event_at_d).alias("last_known_event_type_len"),
        F.round(F.cume_dist().over(w_event_type_by_event_type_len), 3).alias("event_type_len_cume_dist"),
    )
)

result = (
    windowed
    .groupingSets(
        [["event_type_len", "ticket_channel", "last_known_event_type_len"], ["event_type_len"], ["ticket_channel"]],
        "event_type_len", "ticket_channel", "last_known_event_type_len",
    )
    .agg(F.max("max_ticket_csat_so_far").alias("max_max_ticket_csat_so_far"))
    .select(
        "event_type_len",
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "last_known_event_type_len",
        "max_max_ticket_csat_so_far",
    )
)
