from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_csat_by_median_priority_len = Window.partitionBy("csat").orderBy(F.col("median_priority_len").desc(), "channel", "csat", "priority")
w_csat_by_median_priority_len_2 = Window.partitionBy("csat").orderBy(F.col("median_priority_len").desc(), "channel", "csat", "priority").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

totals = (
    scoped
    .filter(F.col("priority_len").between(5, 11))
    .groupBy("channel", "csat", "priority")
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("priority_len", 0.5).alias("median_priority_len"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.round(F.col("median_priority_len") / F.nullif(F.sum("median_priority_len").over(w_csat), F.lit(0)), 4).alias("median_priority_len_share_of_csat"),
        F.round(F.cume_dist().over(w_csat_by_median_priority_len), 3).alias("median_priority_len_cume_dist"),
        F.last("n_rows", ignorenulls=True).over(w_csat_by_median_priority_len_2).alias("last_known_n_rows"),
        F.row_number().over(w_csat_by_median_priority_len).alias("median_priority_len_seq"),
        F.size(F.collect_set("channel").over(w_csat)).alias("distinct_channel_in_csat"),
    )
)

leaders = (
    with_trends
    .filter(F.col("median_priority_len_seq") == 1)
    .drop("median_priority_len_seq")
)

derived = (
    leaders
    .filter(F.size(F.col("channel_list")) >= 2)
    .withColumn("channel_or_unknown", F.coalesce(F.col("channel"), F.lit("UNKNOWN")))
)

result = (
    derived
    .select(
        "channel",
        "csat",
        "median_priority_len_share_of_csat",
        "median_priority_len_cume_dist",
        "last_known_n_rows",
        "channel_or_unknown",
    )
    .orderBy(F.col("csat").asc_nulls_last())
)
