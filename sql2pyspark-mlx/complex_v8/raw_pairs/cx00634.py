from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_has_refund = Window.partitionBy("csat", "has_refund")
w_opened_at_month_agent_id_by = Window.partitionBy(F.month("opened_at"), "agent_id").orderBy(F.col("csat").desc())
w_has_refund_by_closed_at = Window.partitionBy("has_refund").orderBy("closed_at", "id")
w_has_refund_by_closed_at_ties = Window.partitionBy("has_refund").orderBy("closed_at")
w_csat_dense_rank_agent_id_by = Window.partitionBy("csat_dense_rank", "agent_id").orderBy(F.col("requester_id_distance_vs_csat_has_refund_avg").asc_nulls_last(), "id")
w_opened_at_to_date_closed_at = Window.partitionBy(F.to_date("opened_at"), "closed_at_decile")
w_closed_at_decile_by_csat_has = Window.partitionBy("closed_at_decile").orderBy("csat_has_refund_sum_requester_id_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_tickets = (
    df_tickets
    .filter((F.col("priority") != "P2") & (F.col("channel") == "chat"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

sequenced = (
    prepared_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        F.count(F.lit(1)).over(w_csat_has_refund).alias("rows_in_csat_has_refund"),
        F.round(F.col("requester_id_distance") - F.avg("requester_id_distance").over(w_csat_has_refund), 2).alias("requester_id_distance_vs_csat_has_refund_avg"),
        F.dense_rank().over(w_opened_at_month_agent_id_by).alias("csat_dense_rank"),
        F.ntile(10).over(w_has_refund_by_closed_at).alias("closed_at_decile"),
        F.ntile(3).over(w_has_refund_by_closed_at).alias("closed_at_tercile"),
        F.sum("requester_id_distance").over(w_csat_has_refund).alias("csat_has_refund_sum_requester_id_distance"),
        F.round(F.percent_rank().over(w_has_refund_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.round(F.percent_rank().over(w_has_refund_by_closed_at), 3).alias("closed_at_percent_rank_2"),
    )
)

top_rows = sequenced.filter(F.col("csat_dense_rank") <= 5)

windowed = (
    top_rows
    .select(
        "id",
        "agent_id",
        "rows_in_csat_has_refund",
        "requester_id_distance_vs_csat_has_refund_avg",
        "csat_dense_rank",
        "closed_at_decile",
        "closed_at_tercile",
        "csat_has_refund_sum_requester_id_distance",
        "closed_at_percent_rank",
        "closed_at_percent_rank_2",
        F.round(F.percent_rank().over(w_csat_dense_rank_agent_id_by), 3).alias("requester_id_distance_vs_csat_has_refund_avg_per"),
        F.size(F.collect_set("agent_id").over(w_opened_at_to_date_closed_at)).alias("distinct_agent_id_in_opened_at_to_date_closed_at"),
        F.round(F.col("csat_dense_rank") - F.avg("csat_dense_rank").over(w_opened_at_to_date_closed_at), 2).alias("csat_dense_rank_vs_opened_at_to_date_closed_at_d"),
        F.count(F.lit(1)).over(w_opened_at_to_date_closed_at).alias("rows_in_opened_at_to_date_closed_at_decile"),
        F.round(F.col("closed_at_percent_rank_2") - F.avg("closed_at_percent_rank_2").over(w_opened_at_to_date_closed_at), 2).alias("closed_at_percent_rank_2_vs_opened_at_to_date_cl"),
        F.nth_value("closed_at_percent_rank", 2).over(w_closed_at_decile_by_csat_has).alias("second_closed_at_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "rows_in_csat_has_refund",
        "csat_dense_rank",
        "closed_at_decile",
        "closed_at_percent_rank",
        "closed_at_percent_rank_2",
        "requester_id_distance_vs_csat_has_refund_avg_per",
        "distinct_agent_id_in_opened_at_to_date_closed_at",
        "rows_in_opened_at_to_date_closed_at_decile",
        "closed_at_percent_rank_2_vs_opened_at_to_date_cl",
        F.least(F.col("closed_at_decile"), F.col("closed_at_tercile")).alias("least_closed_at_decile_closed_at_tercile"),
    )
    .orderBy(
        F.col("csat_dense_rank").desc(),
        F.col("least_closed_at_decile_closed_at_tercile").desc(),
    )
)
