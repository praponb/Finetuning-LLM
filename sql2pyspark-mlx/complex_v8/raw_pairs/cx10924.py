from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").asc_nulls_last(), "id")
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").asc_nulls_last())
w_priority_agent_id_by_opened = Window.partitionBy("priority", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-3, -1)
w_agent_id_csat_by_csat_runnin = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat_max_3_prior").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .filter(~F.col("priority").isin("P2", "P3"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.lag("csat").over(w_agent_id_csat_by_csat).alias("prev_csat"),
        F.rank().over(w_agent_id_csat_by_csat_ties).alias("csat_rank"),
        F.max("csat").over(w_priority_agent_id_by_opened).alias("csat_max_3_prior"),
        F.sum("csat").over(w_agent_id_csat_by_csat_runnin).alias("running_csat"),
        F.rank().over(w_agent_id_csat_by_csat).alias("csat_rank_2"),
        (F.col("csat") - F.lag("csat").over(w_agent_id_csat_by_csat)).alias("csat_change"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("csat_rank") <= 2) & (F.col("channel") != "email"))
    .drop("csat_rank")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("event_actor_count"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("event_actor_count_2"),
    )
)

profiled = (
    top_per_group.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_actor_count_2"), F.lit(0)).alias("event_actor_count_2"),
    )
)

qualified = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

channel_stats = (
    qualified
    .groupBy("channel")
    .agg(F.avg("prev_csat").alias("avg_prev_csat"))
)

above_average = (
    qualified.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.prev_csat") > F.col("g.avg_prev_csat"))
    .select(
        "s.*",
        "g.avg_prev_csat",
        F.round(F.col("s.prev_csat") - F.col("g.avg_prev_csat"), 2).alias("prev_csat_minus_avg"),
    )
)

unique_rows = (
    above_average
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "prev_csat",
        "csat_max_3_prior",
        "running_csat",
        "event_count",
        "event_actor_count",
        "event_actor_count_2",
        "prev_csat_minus_avg",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy(F.col("event_count").desc(), "id")
)
