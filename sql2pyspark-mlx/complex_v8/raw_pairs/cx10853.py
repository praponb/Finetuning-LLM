from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_by_closed_at = Window.partitionBy(F.month("opened_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_channel_opened_at_year = Window.partitionBy("channel", F.year("opened_at"))
w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_opened_at_month_by_closed_at_2 = Window.partitionBy(F.month("opened_at")).orderBy("closed_at")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_closed_at_year_seq_no_by_req = Window.partitionBy(F.year("closed_at"), "seq_no").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_seq_no = Window.partitionBy("agent_id").orderBy(F.col("seq_no").desc(), "id")
w_closed_at_rank_by_opened_at = Window.partitionBy("closed_at_rank").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("requester_id") > 8))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
    )
)

metrics = (
    prepared
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.last("requester_id", ignorenulls=True).over(w_opened_at_month_by_closed_at).alias("last_known_requester_id"),
        F.lead("requester_id", 3, 0).over(w_agent_id_by_requester_id).alias("next3_requester_id"),
        F.rank().over(w_all_by_closed_at).alias("closed_at_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_opened_at_year), F.lit(0)), 4).alias("requester_id_share_of_channel_opened_at_year"),
        F.max("requester_id").over(w_channel_agent_id).alias("channel_agent_id_max_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_id_by_requester_id)).alias("requester_id_change"),
        F.round(F.percent_rank().over(w_agent_id_by_requester_id), 3).alias("requester_id_percent_rank"),
        F.round(F.avg("requester_id").over(w_channel_agent_id), 2).alias("channel_agent_id_avg_requester_id"),
        F.round(F.cume_dist().over(w_opened_at_month_by_closed_at_2), 3).alias("closed_at_cume_dist"),
        F.rank().over(w_agent_id_by_opened_at).alias("opened_at_rank"),
        F.row_number().over(w_all_by_closed_at).alias("seq_no"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "closed_at_rank",
        "channel_agent_id_max_requester_id",
        "requester_id_change",
        "channel_agent_id_avg_requester_id",
        "closed_at_cume_dist",
        F.row_number().over(w_closed_at_year_seq_no_by_req).alias("rn"),
        F.rank().over(w_closed_at_year_seq_no_by_req).alias("requester_id_rank"),
        F.row_number().over(w_agent_id_by_seq_no).alias("seq_no_seq"),
        F.last("requester_id_change", ignorenulls=True).over(w_closed_at_rank_by_opened_at).alias("last_known_requester_id_change"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "opened_at",
        "closed_at",
        "closed_at_rank",
        "channel_agent_id_max_requester_id",
        "channel_agent_id_avg_requester_id",
        "closed_at_cume_dist",
        "rn",
        "requester_id_rank",
        "seq_no_seq",
        "last_known_requester_id_change",
    )
    .orderBy(F.col("seq_no_seq").desc_nulls_first(), "closed_at")
)
