from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_opened = Window.partitionBy("priority", "agent_id").orderBy("opened_at", "id")
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_median_opened_at_rank = Window.partitionBy("median_opened_at_rank")
w_first_opened_at_month_median = Window.partitionBy(F.month("first_opened_at"), "median_opened_at_rank").orderBy("first_opened_at", "opened_at_rank")
w_first_opened_at_month_median_2 = Window.partitionBy(F.month("first_opened_at"), "median_opened_at_rank").orderBy("first_opened_at", "opened_at_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-02-02 03:00:00").cast("timestamp")) | (F.col("channel") == "chat"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

with_activity = (
    recent_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id").between(2, 25)), "left_anti")
    .filter(F.col("priority").isin("P3", "P1", "P2"))
)

windowed = (
    with_activity
    .select(
        "id",
        "opened_at",
        "priority_channel_label",
        F.rank().over(w_priority_agent_id_by_opened).alias("opened_at_rank"),
        F.lead("opened_at", 3).over(w_all_by_opened_at).alias("next3_opened_at"),
    )
)

totals = (
    windowed
    .groupBy("opened_at_rank")
    .agg(
        F.min("opened_at").alias("first_opened_at"),
        F.percentile_approx("opened_at_rank", 0.5).alias("median_opened_at_rank"),
        F.round(F.avg("opened_at_rank"), 2).alias("avg_opened_at_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("opened_at_rank").alias("avg_opened_at_rank_check"),
    )
    .filter(F.col("avg_opened_at_rank_check") > 2)
    .drop("avg_opened_at_rank_check")
)

with_trends = (
    totals
    .select(
        "opened_at_rank",
        "first_opened_at",
        "median_opened_at_rank",
        "distinct_ids",
        F.max("avg_opened_at_rank").over(w_median_opened_at_rank).alias("median_opened_at_rank_max_avg_opened_at_rank"),
        F.max("opened_at_rank").over(w_median_opened_at_rank).alias("median_opened_at_rank_max_opened_at_rank"),
        F.sum("avg_opened_at_rank").over(w_median_opened_at_rank).alias("median_opened_at_rank_sum_avg_opened_at_rank"),
        F.ntile(3).over(w_first_opened_at_month_median).alias("first_opened_at_tercile"),
        F.sum("opened_at_rank").over(w_first_opened_at_month_median_2).alias("running_opened_at_rank"),
        F.row_number().over(w_first_opened_at_month_median).alias("seq_no"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("seq_no") == 1) & (F.hour(F.col("first_opened_at")).between(10, 14)))
    .drop("seq_no")
)

enriched = (
    first_rows
    .withColumn("median_opened_at_rank_bucket", F.floor(F.col("median_opened_at_rank") / 2) * 2)
)

result = (
    enriched
    .select(
        "opened_at_rank",
        "first_opened_at",
        "median_opened_at_rank",
        "distinct_ids",
        "median_opened_at_rank_max_avg_opened_at_rank",
        "median_opened_at_rank_max_opened_at_rank",
        "median_opened_at_rank_sum_avg_opened_at_rank",
        "first_opened_at_tercile",
        "median_opened_at_rank_bucket",
    )
)
