from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_channels_agent_id = Window.partitionBy("distinct_channels", "agent_id")
w_csat_by_pct_priority_in_p4_p = Window.partitionBy("csat").orderBy("pct_priority_in_p4_p2", "agent_id", "csat", "channel")
w_all_by_pct_priority_in_p4_p2 = Window.orderBy(F.col("pct_priority_in_p4_p2").desc(), "agent_id", "csat", "channel")

scoped = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(8, 12)) | (F.col("requester_id") <= 18))
)

stats = (
    scoped
    .groupBy("agent_id", "csat", "channel")
    .agg(
        F.round(100 * F.count(F.when(F.col("priority").isin("P4", "P2"), 1)) / F.count("*"), 1).alias("pct_priority_in_p4_p2"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.avg("csat").alias("avg_csat_check"),
    )
    .filter(F.col("avg_csat_check") > 2)
    .drop("avg_csat_check")
)

with_trends = (
    stats
    .select(
        "*",
        F.max("csat").over(w_distinct_channels_agent_id).alias("distinct_channels_agent_id_max_csat"),
        (F.col("csat") - F.lag("csat").over(w_csat_by_pct_priority_in_p4_p)).alias("csat_change"),
        F.lead("pct_priority_in_p4_p2").over(w_csat_by_pct_priority_in_p4_p).alias("next_pct_priority_in_p4_p2"),
        (F.col("pct_priority_in_p4_p2") - F.lag("pct_priority_in_p4_p2").over(w_all_by_pct_priority_in_p4_p2)).alias("pct_priority_in_p4_p2_change"),
    )
)

result = (
    with_trends
    .filter(F.col("csat") > 1)
    .select(
        "agent_id",
        "channel",
        "pct_priority_in_p4_p2",
        "channel_list",
        "distinct_channels",
    )
    .orderBy(F.col("agent_id").desc_nulls_first())
)
