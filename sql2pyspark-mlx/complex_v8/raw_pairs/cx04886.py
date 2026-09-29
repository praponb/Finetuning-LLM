from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_quarter_csat_by_cl = Window.partitionBy("closed_at_quarter", "csat").orderBy("closed_at", "id")
w_closed_at_quarter_csat_by_cl_2 = Window.partitionBy("closed_at_quarter", "csat").orderBy("closed_at")
w_all_by_closed_at_quarter_ful = Window.orderBy("closed_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_closed_at_ties = Window.partitionBy("csat").orderBy(F.col("closed_at").desc())
w_csat = Window.partitionBy("csat")
w_closed_at_quarter_priority_b = Window.partitionBy("closed_at_quarter", "priority").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_running = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_quarter_run = Window.orderBy("closed_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_closed_at_quarter = Window.partitionBy("priority", "closed_at_quarter")

valid_tickets = (
    df_tickets
    .filter((F.col("csat") <= 4) | (F.col("requester_id") <= 21))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

windowed = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        "closed_at_quarter",
        F.round(F.cume_dist().over(w_closed_at_quarter_csat_by_cl), 3).alias("closed_at_cume_dist"),
        F.dense_rank().over(w_closed_at_quarter_csat_by_cl_2).alias("closed_at_dense_rank"),
        F.last("closed_at_quarter").over(w_all_by_closed_at_quarter_ful).alias("last_closed_at_quarter"),
        F.round(F.cume_dist().over(w_csat_by_closed_at_ties), 3).alias("closed_at_cume_dist_2"),
        F.lead("requester_id", 3).over(w_closed_at_quarter_csat_by_cl).alias("next3_requester_id"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.first("closed_at").over(w_closed_at_quarter_priority_b).alias("first_closed_at"),
        F.first("requester_id", ignorenulls=True).over(w_all_by_requester_id_running).alias("first_known_requester_id"),
        F.last("closed_at", ignorenulls=True).over(w_all_by_closed_at_quarter_run).alias("last_known_closed_at"),
        F.count(F.lit(1)).over(w_priority_closed_at_quarter).alias("rows_in_priority_closed_at_quarter"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        "closed_at_dense_rank",
        "last_closed_at_quarter",
        "next3_requester_id",
        "first_closed_at",
        "first_known_requester_id",
        "last_known_closed_at",
        "rows_in_priority_closed_at_quarter",
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

result = (
    tagged
    .filter(F.col("first_closed_at") >= F.lit("2025-01-14 03:00:00").cast("timestamp"))
    .select("id", "rows_in_priority_closed_at_quarter")
    .distinct()
    .orderBy("rows_in_priority_closed_at_quarter", F.col("id").asc_nulls_last())
)
