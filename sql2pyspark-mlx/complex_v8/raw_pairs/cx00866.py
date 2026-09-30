from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_by_closed_at_ties = Window.partitionBy("agent_id").orderBy("closed_at")
w_csat = Window.partitionBy("csat")
w_csat_by_closed_at = Window.partitionBy("csat").orderBy("closed_at", "id")
w_agent_id_by_closed_at_dense = Window.partitionBy("agent_id").orderBy("closed_at_dense_rank")
w_closed_at_dense_rank_2 = Window.partitionBy("closed_at_dense_rank_2")
w_agent_id_by_next_closed_at = Window.partitionBy("agent_id").orderBy(F.col("next_closed_at").desc(), "id")
w_agent_id_by_next_closed_at_f = Window.partitionBy("agent_id").orderBy(F.col("next_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_closed_at_runnin = Window.orderBy(F.col("next_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) | (F.col("requester_id") <= 21))
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
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.rank().over(w_agent_id_by_closed_at).alias("closed_at_rank"),
        F.dense_rank().over(w_agent_id_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.dense_rank().over(w_csat_by_closed_at).alias("closed_at_dense_rank_2"),
        F.min("csat").over(w_csat).alias("csat_min_csat"),
        F.round(F.cume_dist().over(w_csat_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.row_number().over(w_csat_by_closed_at).alias("seq_no"),
        F.lead("closed_at").over(w_csat_by_closed_at).alias("next_closed_at"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_id_by_closed_at_dense), 3).alias("closed_at_dense_rank_percent_rank"),
        F.round(F.col("rows_in_csat") / F.nullif(F.sum("rows_in_csat").over(w_closed_at_dense_rank_2), F.lit(0)), 4).alias("rows_in_csat_share_of_closed_at_dense_rank_2"),
        F.row_number().over(w_agent_id_by_next_closed_at).alias("seq_no_2"),
        F.first("closed_at").over(w_agent_id_by_next_closed_at_f).alias("first_closed_at"),
        F.first("csat", ignorenulls=True).over(w_all_by_next_closed_at_runnin).alias("first_known_csat"),
        F.first("closed_at_rank").over(w_agent_id_by_next_closed_at_f).alias("first_closed_at_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("closed_at_rank") <= 9)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_slug",
        "closed_at_dense_rank",
        "rows_in_csat",
        "closed_at_cume_dist",
        "seq_no",
        "next_closed_at",
        "closed_at_dense_rank_percent_rank",
        "first_closed_at",
        "first_closed_at_rank",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 3, "B").otherwise("C").alias("csat_tier"),
    )
    .orderBy(F.col("csat").asc_nulls_last())
)
