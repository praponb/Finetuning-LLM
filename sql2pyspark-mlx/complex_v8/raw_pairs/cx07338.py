from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_closed_at_to_date_priority = Window.partitionBy(F.to_date("closed_at"), "priority")
w_agent_id_by_csat_filled_movi = Window.partitionBy("agent_id").orderBy("csat_filled", "id").rowsBetween(-6, Window.currentRow)
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_csat_by_requester_id_plus_cs = Window.partitionBy("csat").orderBy(F.col("requester_id_plus_csat").desc(), "id")
w_csat_by_requester_id_plus_cs_2 = Window.partitionBy("csat").orderBy(F.col("requester_id_plus_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (~F.col("priority").isin("P4", "P2")))
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
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

ranked = (
    base_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "requester_id_plus_csat",
        F.ntile(3).over(w_all_by_requester_id).alias("requester_id_tercile"),
        F.lead("csat", 2).over(w_all_by_requester_id).alias("next2_csat"),
        F.sum("requester_id").over(w_closed_at_to_date_priority).alias("closed_at_to_date_priority_sum_requester_id"),
        F.round(F.avg("csat").over(w_closed_at_to_date_priority), 2).alias("closed_at_to_date_priority_avg_csat"),
        F.round(F.avg("csat_filled").over(w_agent_id_by_csat_filled_movi), 2).alias("csat_filled_avg_7"),
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
        F.size(F.collect_set("channel").over(w_closed_at_to_date_priority)).alias("distinct_channel_in_closed_at_to_date_priority"),
        F.size(F.collect_set("agent_id").over(w_closed_at_to_date_priority)).alias("distinct_agent_id_in_closed_at_to_date_priority"),
        F.row_number().over(w_csat_by_requester_id_plus_cs).alias("seq_no"),
        F.first("requester_id", ignorenulls=True).over(w_csat_by_requester_id_plus_cs_2).alias("first_known_requester_id"),
        F.row_number().over(w_all_by_requester_id).alias("requester_id_seq"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_csat_by_requester_id_plus_cs))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.col("csat_filled") - F.avg("csat_filled").over(w_closed_at_to_date_priority), 2).alias("csat_filled_vs_closed_at_to_date_priority_avg"),
    )
)

summary = (
    ranked
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("priority").isin("P2", "P1", "P3"), 1).otherwise(0)).alias("n_priority_in_p2_p1_p3"),
    )
)

result = (
    summary
    .select(
        "seq_no",
        "row_count",
        "n_priority_in_p2_p1_p3",
        F.greatest(F.col("seq_no"), F.col("row_count")).alias("greatest_seq_no_row_count"),
    )
    .orderBy(
        F.col("row_count").desc(),
        F.col("greatest_seq_no_row_count").asc_nulls_last(),
    )
)
