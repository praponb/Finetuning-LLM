from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_by_csat_full = Window.partitionBy(F.month("closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month = Window.partitionBy(F.month("closed_at"))
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_opened_at = Window.partitionBy("csat").orderBy("opened_at", "id")

scoped_tickets = (
    df_tickets
    .filter(~F.col("priority").isin("P2", "P4"))
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
    )
)

ranked = (
    scoped_tickets
    .select(
        "*",
        F.last("csat").over(w_closed_at_month_by_csat_full).alias("last_csat"),
        F.sum("csat").over(w_closed_at_month).alias("closed_at_month_sum_csat"),
        F.round(F.avg("requester_id").over(w_closed_at_month), 2).alias("closed_at_month_avg_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_all_by_closed_at)).alias("csat_change"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.first("csat").over(w_all_by_closed_at_full).alias("first_csat"),
        F.count(F.lit(1)).over(w_closed_at_month).alias("rows_in_closed_at_month"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_month), F.lit(0)), 4).alias("requester_id_share_of_closed_at_month"),
        F.min("requester_id").over(w_closed_at_month).alias("closed_at_month_min_requester_id"),
        F.nth_value("closed_at", 2).over(w_all_by_closed_at_full).alias("second_closed_at"),
        F.round(F.cume_dist().over(w_csat_by_opened_at), 3).alias("opened_at_cume_dist"),
    )
)

derived = (
    ranked
    .filter(F.col("first_csat").between(2, 4))
    .withColumn("channel_prefix", F.substring(F.trim(F.col("channel")), 1, 4))
)

result = (
    derived
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
        "last_csat",
        "closed_at_month_avg_requester_id",
        "closed_at_month_min_requester_id",
        "second_closed_at",
        "channel_prefix",
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
    )
    .orderBy(F.col("channel_prefix").asc())
)
