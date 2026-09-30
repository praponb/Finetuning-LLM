from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_by_csat_ful = Window.partitionBy("agent_id", "channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_all_by_requester_id_filled = Window.orderBy("requester_id_filled", "id")
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first())
w_agent_id_by_csat_moving_1 = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_opened_at_full = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_channel_by_csat_tie = Window.partitionBy("agent_id", "channel").orderBy("csat")
w_agent_id_channel_by_csat = Window.partitionBy("agent_id", "channel").orderBy("csat", "id")
w_all_by_requester_id_filled_m = Window.orderBy("requester_id_filled", "id").rowsBetween(-6, Window.currentRow)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id")

prepared = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "bug")) & (~F.col("channel").isin("web", "phone")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.when(F.col("priority").isin("P1", "P4"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.first("requester_id_filled").over(w_agent_id_channel_by_csat_ful).alias("first_requester_id_filled"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_agent_id), F.lit(0)), 4).alias("csat_share_of_csat_agent_id"),
        F.round(F.cume_dist().over(w_all_by_requester_id_filled), 3).alias("requester_id_filled_cume_dist"),
        F.round(F.cume_dist().over(w_agent_id_by_csat_ties), 3).alias("csat_cume_dist"),
        F.round(F.avg("csat").over(w_agent_id_by_csat_moving_1), 2).alias("csat_avg_2"),
        F.nth_value("opened_at", 2).over(w_all_by_opened_at_full).alias("second_opened_at"),
        F.max("csat").over(w_agent_id_by_csat_running).alias("max_csat_so_far"),
        F.round(F.cume_dist().over(w_agent_id_channel_by_csat_tie), 3).alias("csat_cume_dist_2"),
        F.row_number().over(w_agent_id_channel_by_csat).alias("seq_no"),
        F.sum("csat").over(w_all_by_requester_id_filled_m).alias("csat_sum_7"),
        F.ntile(5).over(w_agent_id_by_csat).alias("csat_quintile"),
    )
)

top_per_group = sequenced.filter(F.col("seq_no") == 1)

result = (
    top_per_group
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "requester_id_filled",
        "priority_group",
        "first_requester_id_filled",
        "csat_cume_dist",
        "max_csat_so_far",
        "csat_cume_dist_2",
        "seq_no",
        "csat_sum_7",
        F.concat(F.col("channel"), F.lit("#"), F.col("seq_no").cast("string")).alias("channel_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
    .orderBy("channel")
)
