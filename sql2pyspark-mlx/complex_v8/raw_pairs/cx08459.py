from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_closed_at_to_date = Window.partitionBy("channel", F.to_date("closed_at"))
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")

scoped = df_tickets.filter(F.col("requester_id") <= 21)

sequenced = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") - F.avg("csat").over(w_channel_closed_at_to_date), 2).alias("csat_vs_channel_closed_at_to_date_avg"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.lag("requester_id", 2).over(w_all_by_closed_at).alias("prev2_requester_id"),
    )
)

result = (
    sequenced
    .groupBy("agent_id")
    .pivot("channel", ["web", "phone"])
    .agg(F.max("prev2_requester_id"))
    .orderBy("agent_id")
)
