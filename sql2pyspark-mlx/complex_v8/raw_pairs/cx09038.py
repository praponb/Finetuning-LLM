from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_closed_at = Window.partitionBy("priority").orderBy(F.col("closed_at").desc(), "id")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat_filled = Window.orderBy("csat_filled", "id")
w_priority = Window.partitionBy("priority")
w_agent_id_csat_by_csat_filled = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat_filled").desc(), "id")
w_agent_id_csat_by_csat_filled_2 = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_all_by_csat_filled_ties = Window.orderBy("csat_filled")
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_csat_filled_change = Window.partitionBy("csat_filled_change")
w_csat_filled_by_next_closed_a = Window.partitionBy("csat_filled").orderBy(F.col("next_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-24 01:00:00").cast("timestamp")) | (F.col("requester_id") <= 23))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

sequenced = (
    cleaned_tickets
    .select(
        "*",
        F.lead("closed_at").over(w_priority_by_closed_at).alias("next_closed_at"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.round(F.cume_dist().over(w_all_by_csat_filled), 3).alias("csat_filled_cume_dist"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.lag("csat", 2).over(w_agent_id_csat_by_csat_filled).alias("prev2_csat"),
        (F.col("csat_filled") - F.lag("csat_filled").over(w_all_by_csat_filled)).alias("csat_filled_change"),
        F.lag("closed_at", 3).over(w_all_by_csat_filled).alias("prev3_closed_at"),
        F.last("csat_filled", ignorenulls=True).over(w_agent_id_csat_by_csat_filled_2).alias("last_known_csat_filled"),
        F.dense_rank().over(w_all_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.dense_rank().over(w_all_by_csat_filled_ties).alias("csat_filled_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "csat_filled",
        "channel_clean",
        "next_closed_at",
        "requester_id_vs_agent_id_avg",
        "csat_filled_cume_dist",
        "rows_in_agent_id",
        "prev2_csat",
        "requester_id_dense_rank",
        "closed_at_percent_rank",
        F.sum("csat_filled_cume_dist").over(w_csat_filled_change).alias("csat_filled_change_sum_csat_filled_cume_dist"),
        F.min("requester_id_vs_agent_id_avg").over(w_csat_filled_change).alias("csat_filled_change_min_requester_id_vs_agent_id_"),
        F.sum("requester_id").over(w_csat_filled_change).alias("csat_filled_change_sum_requester_id"),
        F.nth_value("csat_filled", 2).over(w_csat_filled_by_next_closed_a).alias("second_csat_filled"),
        F.count(F.lit(1)).over(w_csat_filled_change).alias("rows_in_csat_filled_change"),
    )
)

result = (
    ranked
    .filter(F.col("requester_id") <= 18)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "csat_filled",
        "requester_id_vs_agent_id_avg",
        "csat_filled_cume_dist",
        "rows_in_agent_id",
        "prev2_csat",
        "requester_id_dense_rank",
        "closed_at_percent_rank",
        "csat_filled_change_sum_csat_filled_cume_dist",
        "csat_filled_change_min_requester_id_vs_agent_id_",
        "csat_filled_change_sum_requester_id",
        "rows_in_csat_filled_change",
        F.substring(F.trim(F.col("channel_clean")), 1, 3).alias("channel_clean_prefix"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
    .orderBy(F.col("csat_filled_cume_dist").desc_nulls_first())
)
