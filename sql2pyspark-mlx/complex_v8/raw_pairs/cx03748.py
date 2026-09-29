from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())
w_event_type = Window.partitionBy("event_type")
w_priority_by_days_since_opene = Window.partitionBy("priority").orderBy("days_since_opened_at", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) & (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

qualified = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()) & (F.year(F.col("su.hired_on")) == 2024), "left_anti")
)

latest = (
    qualified
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail_events = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags_text",
        "s.days_since_opened_at",
        "s.greatest_requester_id_csat",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

metrics = (
    exploded_detail_events
    .select(
        "*",
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_event_type), F.lit(0)), 4).alias("days_since_opened_at_share_of_event_type"),
        F.nth_value("event_at", 2).over(w_priority_by_days_since_opene).alias("second_event_at"),
        F.min("greatest_requester_id_csat").over(w_event_type).alias("event_type_min_greatest_requester_id_csat"),
        F.round(F.col("greatest_requester_id_csat") / F.nullif(F.sum("greatest_requester_id_csat").over(w_event_type), F.lit(0)), 4).alias("greatest_requester_id_csat_share_of_event_type"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "opened_at",
        "tags_text",
        "greatest_requester_id_csat",
        "event_id",
        "event_actor",
        "event_type",
        "event_at",
        "days_since_opened_at_share_of_event_type",
        "second_event_at",
        "greatest_requester_id_csat_share_of_event_type",
        "rows_in_event_type",
        (F.floor(F.col("rows_in_event_type") / 5) * 5).alias("rows_in_event_type_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
    .orderBy(F.col("event_actor").desc(), F.col("priority_slug").desc())
)
