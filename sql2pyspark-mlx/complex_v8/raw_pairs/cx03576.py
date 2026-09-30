from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat_minus = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat_minus_requester_id").desc(), "id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_agent_id_csat_by_csat_minus_2 = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat_minus_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_by_requester_id = Window.partitionBy("csat", "channel").orderBy("requester_id", "id")
w_csat_by_csat_ties = Window.partitionBy("csat").orderBy("csat")
w_csat_channel = Window.partitionBy("csat", "channel")
w_agent_id_csat_by_csat_minus_3 = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat_minus_requester_id").desc())
w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id")
w_csat_by_csat_minus_requester = Window.partitionBy("csat").orderBy(F.col("csat_minus_requester_id_quartile").asc_nulls_last(), "id")
w_csat_by_csat_minus_requester_2 = Window.partitionBy("csat").orderBy(F.col("csat_minus_requester_id_quartile").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_csat_minus_requester_3 = Window.partitionBy("csat").orderBy(F.col("csat_minus_requester_id_quartile").desc_nulls_first(), "id")
w_csat_minus_requester_id_quar = Window.partitionBy("csat_minus_requester_id_quartile")

prepared_tickets = (
    df_tickets
    .filter(F.col("priority") == "P1")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

ranked = (
    prepared_tickets
    .select(
        "id",
        "requester_id",
        "channel",
        "csat",
        "tags",
        "channel_priority_label",
        F.ntile(4).over(w_agent_id_csat_by_csat_minus).alias("csat_minus_requester_id_quartile"),
        F.count(F.lit(1)).over(w_agent_id_channel).alias("rows_in_agent_id_channel"),
        F.min("requester_id").over(w_agent_id_channel).alias("agent_id_channel_min_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_all_by_requester_id)).alias("csat_change"),
        F.max("csat_minus_requester_id").over(w_channel_agent_id).alias("channel_agent_id_max_csat_minus_requester_id"),
        F.min("csat").over(w_agent_id_channel).alias("agent_id_channel_min_csat"),
        F.first("requester_id", ignorenulls=True).over(w_agent_id_csat_by_csat_minus_2).alias("first_known_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_csat_channel_by_requester_id)).alias("requester_id_change"),
        F.round(F.cume_dist().over(w_csat_by_csat_ties), 3).alias("csat_cume_dist"),
        F.sum("csat_minus_requester_id").over(w_csat_channel).alias("csat_channel_sum_csat_minus_requester_id"),
        F.round(F.cume_dist().over(w_agent_id_csat_by_csat_minus_3), 3).alias("csat_minus_requester_id_cume_dist"),
        F.sum("csat").over(w_agent_id_channel).alias("agent_id_channel_sum_csat"),
        F.round(F.percent_rank().over(w_agent_id_csat_by_csat_minus_3), 3).alias("csat_minus_requester_id_percent_rank"),
        F.max("csat").over(w_channel_agent_id).alias("channel_agent_id_max_csat"),
        F.dense_rank().over(w_csat_channel_by_requester_id).alias("requester_id_dense_rank"),
        (F.col("requester_id") - F.lag("requester_id").over(w_csat_by_csat)).alias("requester_id_change_2"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "csat",
        "channel_priority_label",
        "csat_minus_requester_id_quartile",
        "rows_in_agent_id_channel",
        "agent_id_channel_min_requester_id",
        "csat_change",
        "agent_id_channel_min_csat",
        "requester_id_change",
        "csat_channel_sum_csat_minus_requester_id",
        "csat_minus_requester_id_cume_dist",
        "agent_id_channel_sum_csat",
        "csat_minus_requester_id_percent_rank",
        "requester_id_dense_rank",
        F.lag("agent_id_channel_min_csat", 3).over(w_csat_by_csat_minus_requester).alias("prev3_agent_id_channel_min_csat"),
        F.first("agent_id_channel_min_csat", ignorenulls=True).over(w_csat_by_csat_minus_requester_2).alias("first_known_agent_id_channel_min_csat"),
        F.ntile(3).over(w_csat_by_csat_minus_requester_3).alias("csat_minus_requester_id_quartile_tercile"),
        F.min("agent_id_channel_min_csat").over(w_csat_by_csat_minus_requester_2).alias("min_agent_id_channel_min_csat_so_far"),
        F.round(F.col("csat_change") / F.nullif(F.sum("csat_change").over(w_csat_minus_requester_id_quar), F.lit(0)), 4).alias("csat_change_share_of_csat_minus_requester_id_qua"),
        F.first("requester_id_change", ignorenulls=True).over(w_csat_by_csat_minus_requester_2).alias("first_known_requester_id_change"),
        F.min("requester_id_change").over(w_csat_minus_requester_id_quar).alias("csat_minus_requester_id_quartile_min_requester_i"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "channel_priority_label",
        "csat_minus_requester_id_quartile",
        "agent_id_channel_min_requester_id",
        "csat_change",
        "agent_id_channel_min_csat",
        "requester_id_change",
        "csat_channel_sum_csat_minus_requester_id",
        "csat_minus_requester_id_cume_dist",
        "csat_minus_requester_id_percent_rank",
        "prev3_agent_id_channel_min_csat",
        "min_agent_id_channel_min_csat_so_far",
        "csat_change_share_of_csat_minus_requester_id_qua",
        "csat_minus_requester_id_quartile_min_requester_i",
    )
    .orderBy("csat_change_share_of_csat_minus_requester_id_qua", "id")
    .limit(5)
)
