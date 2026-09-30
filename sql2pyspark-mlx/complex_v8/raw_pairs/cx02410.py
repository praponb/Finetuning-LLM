from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc())
w_ticket_channel_ticket_id_by = Window.partitionBy("ticket_channel", "ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_id_by_ticket_requeste_2 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_opened_at_month = Window.partitionBy(F.month("ticket_opened_at"))
w_ticket_id_by_ticket_requeste_3 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_requester_id").desc(), "id")
w_event_type_ticket_channel = Window.partitionBy("event_type", "ticket_channel")
w_ticket_opened_at_year_ticket = Window.partitionBy(F.year("ticket_opened_at"), "ticket_channel").orderBy("ticket_requester_id_share_of_ticket_opened_at_mo", "id").rowsBetween(-6, Window.currentRow)
w_ticket_channel_ticket_id = Window.partitionBy("ticket_channel", "ticket_id")
w_ticket_opened_at_year_ticket_2 = Window.partitionBy(F.year("ticket_opened_at"), "ticket_channel").orderBy("ticket_requester_id_share_of_ticket_opened_at_mo", "id")

prepared = (
    df_ticket_events
    .filter((F.col("event_type") != "assigned") & (F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_label",
        "s.actor_clean",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w_ticket_id_by_ticket_requeste).alias("ticket_requester_id_dense_rank"),
        F.lead("ticket_requester_id").over(w_ticket_channel_ticket_id_by).alias("next_ticket_requester_id"),
        F.nth_value("ticket_requester_id", 2).over(w_ticket_id_by_ticket_requeste_2).alias("second_ticket_requester_id"),
        F.size(F.collect_set("event_type").over(w_ticket_opened_at_month)).alias("distinct_event_type_in_ticket_opened_at_month"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_opened_at_month), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_opened_at_mo"),
        F.round(F.cume_dist().over(w_ticket_id_by_ticket_requeste_3), 3).alias("ticket_requester_id_cume_dist"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_ticket_channel), 2).alias("ticket_requester_id_vs_event_type_ticket_channel"),
    )
)

first_rows = (
    ranked
    .filter(F.col("ticket_requester_id_dense_rank") == 1)
    .drop("ticket_requester_id_dense_rank")
)

metrics = (
    first_rows
    .select(
        "*",
        F.max("ticket_requester_id").over(w_ticket_opened_at_year_ticket).alias("ticket_requester_id_max_7"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_channel_ticket_id), 2).alias("ticket_requester_id_vs_ticket_channel_ticket_id_"),
        F.round(F.col("next_ticket_requester_id") / F.nullif(F.sum("next_ticket_requester_id").over(w_ticket_channel_ticket_id), F.lit(0)), 4).alias("next_ticket_requester_id_share_of_ticket_channel"),
        F.count(F.lit(1)).over(w_ticket_channel_ticket_id).alias("rows_in_ticket_channel_ticket_id"),
        F.lead("next_ticket_requester_id", 3).over(w_ticket_opened_at_year_ticket_2).alias("next3_next_ticket_requester_id"),
        F.round(F.col("second_ticket_requester_id") / F.nullif(F.sum("second_ticket_requester_id").over(w_ticket_channel_ticket_id), F.lit(0)), 4).alias("second_ticket_requester_id_share_of_ticket_chann"),
    )
)

result = (
    metrics
    .filter(F.col("ticket_requester_id_cume_dist").between(0.1, 0.7))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_label",
        "ticket_opened_at",
        "ticket_channel",
        "next_ticket_requester_id",
        "distinct_event_type_in_ticket_opened_at_month",
        "ticket_requester_id_share_of_ticket_opened_at_mo",
        "ticket_requester_id_cume_dist",
        "ticket_requester_id_max_7",
        "ticket_requester_id_vs_ticket_channel_ticket_id_",
        "next_ticket_requester_id_share_of_ticket_channel",
        "rows_in_ticket_channel_ticket_id",
        "second_ticket_requester_id_share_of_ticket_chann",
        F.initcap(F.trim(F.col("event_at_label"))).alias("event_at_label_clean"),
    )
)
