from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_csat = Window.partitionBy("csat")

filtered_tickets = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("channel").isin("email", "phone")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.abs(F.col("requester_id") - 15).alias("requester_id_distance"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

screened = (
    filtered_tickets
    .crossJoin(filtered_tickets.agg(F.avg("requester_id_distance").alias("overall_avg_requester_id_distance")))
    .filter(F.col("requester_id_distance") > F.col("overall_avg_requester_id_distance"))
    .drop("overall_avg_requester_id_distance")
)

with_trends = (
    screened
    .select(
        "*",
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.ntile(10).over(w_all_by_closed_at).alias("closed_at_decile"),
        F.round(F.col("requester_id_distance") / F.nullif(F.sum("requester_id_distance").over(w_csat), F.lit(0)), 4).alias("requester_id_distance_share_of_csat"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
    )
)

result = (
    with_trends
    .rollup("channel", "priority", "closed_at_decile")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("closed_at_decile") / F.nullif(F.sum("requester_id_distance"), F.lit(0)), 3).alias("closed_at_decile_to_requester_id_distance_ratio"),
        F.max("closed_at").alias("last_closed_at"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "closed_at_decile",
        "grouping_level",
        "closed_at_decile_to_requester_id_distance_ratio",
        "last_closed_at",
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
        F.col("closed_at_decile").asc_nulls_last(),
    )
)
