from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_opened = Window.partitionBy("priority", "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_priority_agent_id_by_opened_2 = Window.partitionBy("priority", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_moving_2 = Window.orderBy("opened_at", "id").rowsBetween(-2, Window.currentRow)
w_priority = Window.partitionBy("priority")

prepared_tickets = (
    df_tickets
    .filter((F.col("priority") == "P1") | (F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
    )
)

with_trends = (
    prepared_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "closed_at_year",
        F.lead("requester_id", 3, 0).over(w_priority_agent_id_by_opened).alias("next3_requester_id"),
        F.min("requester_id").over(w_priority_agent_id_by_opened_2).alias("min_requester_id_so_far"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id_priority_by_opened).alias("running_count"),
        F.max("next3_requester_id").over(w_all_by_opened_at_moving_2).alias("next3_requester_id_max_3"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority), 2).alias("requester_id_vs_priority_avg"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "closed_at_year",
        "min_requester_id_so_far",
        "next3_requester_id_max_3",
        "requester_id_vs_priority_avg",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy("priority_clean", "id")
    .limit(20)
)
