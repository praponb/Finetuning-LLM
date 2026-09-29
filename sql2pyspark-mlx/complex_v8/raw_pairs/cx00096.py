from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_quarter_by_opened = Window.partitionBy("closed_at_quarter").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_quarter_channel = Window.partitionBy("closed_at_quarter", "channel")
w_closed_at_quarter_by_opened_2 = Window.partitionBy("closed_at_quarter").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

priority_stats = (
    recent_tickets
    .groupBy("priority")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    recent_tickets.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

with_trends = (
    outliers
    .select(
        "*",
        F.nth_value("requester_id", 2).over(w_closed_at_quarter_by_opened).alias("second_requester_id"),
        F.min("requester_id").over(w_closed_at_quarter_channel).alias("closed_at_quarter_channel_min_requester_id"),
        F.sum("requester_id").over(w_closed_at_quarter_by_opened_2).alias("running_requester_id"),
    )
)

result = (
    with_trends
    .groupBy("closed_at_quarter")
    .pivot("priority", ["P2", "P3", "P4", "P1"])
    .agg(F.count("channel_len"))
    .orderBy("closed_at_quarter")
)
