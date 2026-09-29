from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel").orderBy("opened_at_hour", "id")
w_agent_id_by_opened_at_hour = Window.partitionBy("agent_id").orderBy("opened_at_hour", "id")
w_agent_id_opened_at_hour = Window.partitionBy("agent_id", "opened_at_hour")
w_priority = Window.partitionBy("priority")
w_all_by_requester_id_filled_r = Window.orderBy(F.col("requester_id_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(9, 15)) | (F.month(F.col("opened_at")).isin(2, 3)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

unique_rows = (
    base
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    unique_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "opened_at_hour",
        "requester_id_filled",
        F.ntile(4).over(w_latest).alias("opened_at_hour_quartile"),
        F.lag("requester_id").over(w_agent_id_by_opened_at_hour).alias("prev_requester_id"),
        F.dense_rank().over(w_agent_id_by_opened_at_hour).alias("opened_at_hour_dense_rank"),
        F.round(F.col("requester_id_filled") - F.avg("requester_id_filled").over(w_agent_id_opened_at_hour), 2).alias("requester_id_filled_vs_agent_id_opened_at_hour_a"),
        F.round(F.col("opened_at_hour") / F.nullif(F.sum("opened_at_hour").over(w_agent_id_opened_at_hour), F.lit(0)), 4).alias("opened_at_hour_share_of_agent_id_opened_at_hour"),
        F.count(F.lit(1)).over(w_agent_id_opened_at_hour).alias("rows_in_agent_id_opened_at_hour"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.col("requester_id_filled") - F.avg("requester_id_filled").over(w_priority), 2).alias("requester_id_filled_vs_priority_avg"),
        F.min("opened_at_hour_quartile").over(w_all_by_requester_id_filled_r).alias("min_opened_at_hour_quartile_so_far"),
    )
)

with_details_agent = (
    with_trends.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.requester_id_filled",
        "s.prev_requester_id",
        "s.rows_in_agent_id_opened_at_hour",
        "s.requester_id_filled_vs_priority_avg",
        "s.min_opened_at_hour_quartile_so_far",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

joined = (
    with_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.count("*").alias("ticket_count"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

with_counts = (
    joined.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_opened_at",
    )
)

result = (
    with_counts
    .filter(F.col("min_opened_at_hour_quartile_so_far") > 41)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "requester_id_filled",
        "prev_requester_id",
        "min_opened_at_hour_quartile_so_far",
        "agent_team",
        "agent_tier",
        "last_ticket_opened_at",
        "ticket_count",
        "first_ticket_opened_at",
        (F.col("prev_requester_id") - F.col("ticket_count")).alias("prev_requester_id_minus_ticket_count"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
    .orderBy(F.col("first_ticket_opened_at").desc_nulls_first())
)
