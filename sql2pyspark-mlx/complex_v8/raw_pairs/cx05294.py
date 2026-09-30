from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_requester_id_tier_agent_id = Window.partitionBy("requester_id_tier", "agent_id")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_priority_channel_by_closed_a = Window.partitionBy("priority", "channel").orderBy("closed_at", "id")
w_channel_closed_at_month_by_o = Window.partitionBy("channel", F.month("closed_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tickets
    .filter((F.col("agent_id") >= 7) & (F.col("opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 21, "A").when(F.col("requester_id") >= 8, "B").otherwise("C").alias("requester_id_tier"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "requester_id_tier",
        (F.col("csat") - F.lag("csat").over(w_all_by_closed_at)).alias("csat_change"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_requester_id_tier_agent_id), F.lit(0)), 4).alias("csat_share_of_requester_id_tier_agent_id"),
        F.size(F.collect_set("channel").over(w_requester_id_tier_agent_id)).alias("distinct_channel_in_requester_id_tier_agent_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_csat))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.avg("csat").over(w_agent_id_csat), 2).alias("agent_id_csat_avg_csat"),
        F.row_number().over(w_all_by_csat).alias("csat_seq"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_csat), F.lit(0)), 4).alias("requester_id_share_of_agent_id_csat"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_opened_at_2"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_priority_channel_by_closed_a))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.avg("requester_id").over(w_requester_id_tier_agent_id), 2).alias("requester_id_tier_agent_id_avg_requester_id"),
        F.nth_value("opened_at", 2).over(w_channel_closed_at_month_by_o).alias("second_opened_at"),
    )
)

top_rows = (
    sequenced
    .filter((F.col("csat_seq") == 1) & (F.col("requester_id") > 7))
    .drop("csat_seq")
)

result = (
    top_rows
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "tags",
        "distinct_channel_in_requester_id_tier_agent_id",
        "agent_id_csat_avg_csat",
        "requester_id_share_of_agent_id_csat",
        "minutes_since_prev_opened_at_2",
        "minutes_since_prev_closed_at",
        "second_opened_at",
        F.lower(F.trim(F.col("requester_id_tier"))).alias("requester_id_tier_clean"),
        F.concat_ws(" / ", F.col("channel"), F.col("requester_id_tier")).alias("channel_requester_id_tier_label"),
    )
    .orderBy(F.col("id").desc())
)
