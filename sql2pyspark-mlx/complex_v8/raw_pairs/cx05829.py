from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_priority_by = Window.partitionBy(F.date_trunc("month", "closed_at"), "priority").orderBy("closed_at")
w_has_refund = Window.partitionBy("has_refund")
w_has_refund_by_requester_id = Window.partitionBy("has_refund").orderBy("requester_id", "id")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_closed_at_month_priority_by_2 = Window.partitionBy(F.date_trunc("month", "closed_at"), "priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_has_refund_by_requester_id_f = Window.partitionBy("has_refund").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")

valid = (
    df_tickets
    .filter(F.col("requester_id") >= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.rank().over(w_closed_at_month_priority_by).alias("closed_at_rank"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_has_refund), 2).alias("requester_id_vs_has_refund_avg"),
        F.lead("requester_id", 2).over(w_has_refund_by_requester_id).alias("next2_requester_id"),
        F.count(F.lit(1)).over(w_priority_agent_id).alias("rows_in_priority_agent_id"),
        F.size(F.collect_set("priority").over(w_has_refund)).alias("distinct_priority_in_has_refund"),
        F.nth_value("requester_id", 2).over(w_closed_at_month_priority_by_2).alias("second_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_has_refund_by_requester_id)).alias("requester_id_change"),
        F.first("closed_at").over(w_has_refund_by_requester_id_f).alias("first_closed_at"),
        F.row_number().over(w_has_refund_by_requester_id).alias("seq_no"),
        F.first("requester_id").over(w_closed_at_month_priority_by_2).alias("first_requester_id"),
    )
)

annotated = (
    sequenced
    .select(
        "*",
        F.to_date(F.col("first_closed_at")).alias("first_closed_at_date"),
        F.abs(F.col("closed_at_rank") - 7).alias("closed_at_rank_distance"),
        F.coalesce(F.col("requester_id_change"), F.lit(0)).alias("requester_id_change_filled"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        "has_refund",
        "closed_at_label",
        "requester_id_vs_has_refund_avg",
        "next2_requester_id",
        "rows_in_priority_agent_id",
        "distinct_priority_in_has_refund",
        "requester_id_change",
        "first_closed_at",
        "seq_no",
        "first_requester_id",
        "closed_at_rank_distance",
        "requester_id_change_filled",
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.round(F.col("requester_id_change_filled") / F.nullif(F.sum("requester_id_change_filled").over(w_agent_id), F.lit(0)), 4).alias("requester_id_change_filled_share_of_agent_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        "has_refund",
        "closed_at_label",
        "next2_requester_id",
        "rows_in_priority_agent_id",
        "distinct_priority_in_has_refund",
        "requester_id_change",
        "seq_no",
        "first_requester_id",
        "closed_at_rank_distance",
        "requester_id_change_filled",
        "distinct_priority_in_agent_id",
        "requester_id_change_filled_share_of_agent_id",
    )
    .orderBy("seq_no")
)
