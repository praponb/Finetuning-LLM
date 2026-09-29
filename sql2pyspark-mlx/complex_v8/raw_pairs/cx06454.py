from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_csat_opened_at_month = Window.partitionBy("csat", F.month("opened_at"))
w_csat_by_opened_at = Window.partitionBy("csat").orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc())
w_closed_at_month_agent_id = Window.partitionBy(F.month("closed_at"), "agent_id")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_csat_by_opened_at_ties = Window.partitionBy("csat").orderBy(F.col("opened_at").desc_nulls_first())
w_agent_id_by_csat_ties = Window.partitionBy("agent_id").orderBy(F.col("csat").desc())
w_all_by_closed_at_month_agent = Window.orderBy(F.col("closed_at_month_agent_id_max_csat").desc_nulls_first())
w_prev_csat_by_csat_share_of_c = Window.partitionBy("prev_csat").orderBy("csat_share_of_csat_opened_at_month", "id").rowsBetween(-2, -1)
w_prev_csat_agent_id = Window.partitionBy("prev_csat", "agent_id")
w_prev_csat_by_csat_share_of_c_2 = Window.partitionBy("prev_csat").orderBy("csat_share_of_csat_opened_at_month", "id")

base = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

with_details_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.channel_clean",
        "s.channel_prefix",
        "su.agent_name",
    )
)

with_windows = (
    with_details_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "channel_clean",
        "agent_name",
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.round(F.avg("csat").over(w_csat_opened_at_month), 2).alias("csat_opened_at_month_avg_csat"),
        F.lag("opened_at", 3).over(w_csat_by_opened_at).alias("prev3_opened_at"),
        F.round(F.cume_dist().over(w_agent_id_csat_by_csat_ties), 3).alias("csat_cume_dist"),
        F.min("csat").over(w_closed_at_month_agent_id).alias("closed_at_month_agent_id_min_csat"),
        F.count(F.lit(1)).over(w_closed_at_month_agent_id).alias("rows_in_closed_at_month_agent_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.max("csat").over(w_closed_at_month_agent_id).alias("closed_at_month_agent_id_max_csat"),
        F.round(F.percent_rank().over(w_csat_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_opened_at_month), 2).alias("csat_vs_csat_opened_at_month_avg"),
        F.rank().over(w_agent_id_by_csat_ties).alias("csat_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_opened_at_month), F.lit(0)), 4).alias("csat_share_of_csat_opened_at_month"),
        F.min("csat").over(w_csat_opened_at_month).alias("csat_opened_at_month_min_csat"),
        F.lag("csat").over(w_all_by_closed_at).alias("prev_csat"),
    )
)

annotated = (
    with_windows
    .withColumn("agent_name_prefix", F.substring(F.trim(F.col("agent_name")), 1, 2))
)

with_trends = (
    annotated
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel_clean",
        "closed_at_dense_rank",
        "csat_opened_at_month_avg_csat",
        "closed_at_month_agent_id_min_csat",
        "rows_in_closed_at_month_agent_id",
        "opened_at_percent_rank",
        "csat_rank",
        F.round(F.percent_rank().over(w_all_by_closed_at_month_agent), 3).alias("closed_at_month_agent_id_max_csat_percent_rank"),
        F.round(F.avg("csat").over(w_prev_csat_by_csat_share_of_c), 2).alias("csat_avg_2_prior"),
        F.round(F.col("opened_at_percent_rank") / F.nullif(F.sum("opened_at_percent_rank").over(w_prev_csat_agent_id), F.lit(0)), 4).alias("opened_at_percent_rank_share_of_prev_csat_agent_"),
        F.row_number().over(w_prev_csat_by_csat_share_of_c_2).alias("seq_no"),
    )
)

result = (
    with_trends
    .filter(F.col("csat_opened_at_month_avg_csat").between(1.4, 4.6))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel_clean",
        "closed_at_dense_rank",
        "csat_opened_at_month_avg_csat",
        "closed_at_month_agent_id_min_csat",
        "rows_in_closed_at_month_agent_id",
        "opened_at_percent_rank",
        "csat_rank",
        "closed_at_month_agent_id_max_csat_percent_rank",
        "opened_at_percent_rank_share_of_prev_csat_agent_",
        "seq_no",
    )
    .orderBy(F.col("opened_at").desc())
)
