from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hours_since_open_runn = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_latest = Window.partitionBy("agent_tier").orderBy(F.col("last_known_agent_hired_on").desc(), F.col("id").desc())

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

combined_details_agent = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

metrics = (
    combined_details_agent
    .select(
        "*",
        F.min("hours_since_open").over(w_ticket_id).alias("ticket_id_min_hours_since_open"),
        F.max("hours_since_open").over(w_all_by_agent_hired_on_runnin).alias("max_hours_since_open_so_far"),
        F.last("agent_hired_on", ignorenulls=True).over(w_all_by_hours_since_open_runn).alias("last_known_agent_hired_on"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on), 3).alias("agent_hired_on_cume_dist"),
        F.ntile(4).over(w_all_by_agent_hired_on).alias("agent_hired_on_quartile"),
    )
)

latest_per_key = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "agent_tier",
        "agent_hired_on",
        "agent_name",
        "ticket_id_min_hours_since_open",
        "max_hours_since_open_so_far",
        "last_known_agent_hired_on",
        "agent_hired_on_cume_dist",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.date_format(F.col("agent_hired_on"), "yyyy-MM-dd").alias("agent_hired_on_label"),
    )
    .orderBy(
        F.col("max_hours_since_open_so_far").desc_nulls_first(),
        F.col("last_known_agent_hired_on").desc_nulls_first(),
    )
)
