from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_or_unknown_by_opene = Window.partitionBy("priority_or_unknown").orderBy("opened_at", "id")
w_priority_or_unknown_agent_id = Window.partitionBy("priority_or_unknown", "agent_id")
w_agent_id_channel_by_greatest = Window.partitionBy("agent_id", "channel").orderBy("greatest_csat_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") < 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_priority_or_unknown_by_opene), 3).alias("opened_at_percent_rank"),
        F.round(F.col("greatest_csat_requester_id") / F.nullif(F.sum("greatest_csat_requester_id").over(w_priority_or_unknown_agent_id), F.lit(0)), 4).alias("greatest_csat_requester_id_share_of_priority_or_"),
        F.nth_value("greatest_csat_requester_id", 2).over(w_agent_id_channel_by_greatest).alias("second_greatest_csat_requester_id"),
        F.size(F.collect_set("channel").over(w_priority_or_unknown_agent_id)).alias("distinct_channel_in_priority_or_unknown_agent_id"),
    )
)

derived = (
    with_trends
    .filter(F.col("opened_at_percent_rank") >= 0.2)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "greatest_csat_requester_id",
        "priority_or_unknown",
        "opened_at_percent_rank",
        "greatest_csat_requester_id_share_of_priority_or_",
        "second_greatest_csat_requester_id",
        F.lower(F.regexp_replace(F.trim(F.col("priority_or_unknown")), " ", "_")).alias("priority_or_unknown_slug"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("greatest_csat_requester_id").alias("greatest_csat_requester_id_v"),
        F.col("second_greatest_csat_requester_id").alias("second_greatest_csat_requester_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["greatest_csat_requester_id_v", "second_greatest_csat_requester_id_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
