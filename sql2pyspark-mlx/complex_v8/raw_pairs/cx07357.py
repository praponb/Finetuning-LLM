from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_priority_distinct_channels_b = Window.partitionBy("priority", "distinct_channels").orderBy("cnt")
w_priority_distinct_channels_b_2 = Window.partitionBy("priority", "distinct_channels").orderBy("cnt", "priority", "channel").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

summary = (
    base
    .groupBy("priority", "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.min("csat").alias("min_csat"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("priority") == "P2", 1)) / F.count("*"), 1).alias("pct_priority_p2"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum("csat").alias("sum_csat_check"),
    )
    .filter(F.col("sum_csat_check") > 2)
    .drop("sum_csat_check")
)

with_trends = (
    summary
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.round(F.avg("distinct_channels").over(w_distinct_ids), 2).alias("distinct_ids_avg_distinct_channels"),
        F.round(F.percent_rank().over(w_priority_distinct_channels_b), 3).alias("cnt_percent_rank"),
        F.sum("distinct_channels").over(w_priority_distinct_channels_b_2).alias("running_distinct_channels"),
    )
)

result = (
    with_trends
    .select(
        "priority",
        "channel",
        "cnt",
        "min_csat",
        "distinct_ids",
        "pct_priority_p2",
        "distinct_channels",
        "rows_in_distinct_ids",
        "distinct_ids_avg_distinct_channels",
        "cnt_percent_rank",
        "running_distinct_channels",
        (F.col("distinct_ids") + F.col("rows_in_distinct_ids")).alias("distinct_ids_plus_rows_in_distinct_ids"),
        F.least(F.col("distinct_channels"), F.col("rows_in_distinct_ids")).alias("least_distinct_channels_rows_in_distinct_ids"),
    )
    .orderBy(F.col("pct_priority_p2").desc())
)
