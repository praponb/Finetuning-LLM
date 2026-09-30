from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").asc_nulls_last())
w_channel = Window.partitionBy("channel")
w_all_by_closed_at_running = Window.orderBy(F.col("closed_at").asc_nulls_last(), "id", "tag").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id", "tag")
w_tag = Window.partitionBy("tag")
w_distinct_agent_id_in_channel = Window.partitionBy("distinct_agent_id_in_channel").orderBy("requester_id", "id", "tag")
w_rn_tag_by_closed_at_cume_dis = Window.partitionBy("rn", "tag").orderBy(F.col("closed_at_cume_dist").desc(), "id", "tag").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter((F.col("csat") >= 3) & (F.col("priority") != "P2"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
    )
)

exploded = (
    cleaned
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "closed_at_day",
        F.explode("tags").alias("tag"),
    )
)

ranked = (
    exploded
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "closed_at_day",
        "tag",
        F.round(F.cume_dist().over(w_all_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel), F.lit(0)), 4).alias("requester_id_share_of_channel"),
        F.first("closed_at_day", ignorenulls=True).over(w_all_by_closed_at_running).alias("first_known_closed_at_day"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.row_number().over(w_all_by_requester_id).alias("rn"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.round(F.col("closed_at_cume_dist") / F.nullif(F.sum("closed_at_cume_dist").over(w_tag), F.lit(0)), 4).alias("closed_at_cume_dist_share_of_tag"),
        F.ntile(4).over(w_distinct_agent_id_in_channel).alias("requester_id_quartile"),
        F.count(F.lit(1)).over(w_tag).alias("rows_in_tag"),
        F.round(F.col("rn") - F.avg("rn").over(w_tag), 2).alias("rn_vs_tag_avg"),
        F.min("rn").over(w_rn_tag_by_closed_at_cume_dis).alias("min_rn_so_far"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "closed_at",
        "tag",
        "closed_at_cume_dist",
        "rows_in_channel",
        "requester_id_share_of_channel",
        "rn",
        "requester_id_quartile",
        "min_rn_so_far",
        F.abs(F.col("rn_vs_tag_avg") - 3.0).alias("rn_vs_tag_avg_distance"),
    )
    .orderBy(
        F.col("closed_at_cume_dist").desc(),
        F.col("requester_id_quartile").desc(),
    )
)
