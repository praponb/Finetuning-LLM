from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "ticket_id").orderBy(F.col("first_event_at").desc(), F.col("id").desc())
w_event_count_by_event_count_r = Window.partitionBy("event_count").orderBy("event_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_by_event_count_p = Window.partitionBy("event_count").orderBy("event_count", "id").rowsBetween(-2, -1)
w_event_count_actor = Window.partitionBy("event_count", "actor")
w_ticket_id_by_first_event_at = Window.partitionBy("ticket_id").orderBy("first_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_first_event_at_2 = Window.partitionBy("ticket_id").orderBy("first_event_at", "id")
w_event_count = Window.partitionBy("event_count")

scoped = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 11)) & (F.col("ticket_id") <= 56))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

latest = (
    with_counts
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest
    .select(
        "*",
        F.sum("event_count").over(w_event_count_by_event_count_r).alias("running_event_count"),
        F.sum("event_count").over(w_event_count_by_event_count_p).alias("event_count_sum_2_prior"),
        F.count(F.lit(1)).over(w_event_count_actor).alias("rows_in_event_count_actor"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_id_by_first_event_at).alias("running_count"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_first_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.col("running_event_count") / F.nullif(F.sum("running_event_count").over(w_event_count), F.lit(0)), 4).alias("running_event_count_share_of_event_count"),
    )
)

with_parent_ticket = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "minutes_since_prev_event_at",
        "running_event_count_share_of_event_count",
    )
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_event_at", "running_event_count_share_of_event_count"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
